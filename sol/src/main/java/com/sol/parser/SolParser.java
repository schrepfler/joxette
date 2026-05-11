package com.sol.parser;

import com.sol.engine.SolOperation;
import com.sol.engine.SolOperation.*;
import com.sol.expr.Expr;

import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hand-written recursive-descent parser for SOL queries.
 *
 * <p>A query is a newline-separated sequence of operations:
 * <pre>
 *   match A(event1) >> * >> B(event2)
 *   if duration(A, B) < 5min
 *   filter MATCHED
 *   set result_dim = duration(A, B)
 * </pre>
 *
 * <p>Throws {@link SolParseException} on any syntax error.
 */
public final class SolParser {

    private final TokenStream ts;

    private SolParser(String query) {
        this.ts = new TokenStream(query);
    }

    public static List<SolOperation> parse(String query) {
        return new SolParser(query.strip()).parseOperations();
    }

    // -----------------------------------------------------------------------
    // Top-level operation parsing
    // -----------------------------------------------------------------------

    private List<SolOperation> parseOperations() {
        List<SolOperation> ops = new ArrayList<>();
        while (ts.hasMore()) {
            String kw = ts.peekKeyword();
            SolOperation op = switch (kw) {
                case "match"  -> parseMatchOrMatchSplit();
                case "filter" -> parseFilter();
                case "set"    -> parseSet();
                case "replace" -> parseReplace();
                case "combine" -> parseCombine();
                default -> throw new SolParseException("Unknown operation keyword: " + kw);
            };
            ops.add(op);
        }
        return ops;
    }

    // -----------------------------------------------------------------------
    // MATCH / MATCH SPLIT
    // -----------------------------------------------------------------------

    private SolOperation parseMatchOrMatchSplit() {
        ts.consume("match");
        boolean split = ts.tryConsume("split");
        List<PatternElement> pattern = parsePattern();
        Expr condition = null;
        if (ts.tryConsume("if")) {
            condition = parseExpr();
        }
        return split ? new MatchSplitOp(pattern, condition) : new MatchOp(pattern, condition);
    }

    private List<PatternElement> parsePattern() {
        List<PatternElement> elements = new ArrayList<>();

        // start anchor
        if (ts.tryConsume("start")) {
            elements.add(new PatternElement(null, List.of(), List.of(), Quantifier.ONE, Anchor.START));
            if (!ts.tryConsume(">>")) return elements;
        }

        while (ts.hasMore() && !ts.peekKeyword().equals("if")
                && !isOperationKeyword(ts.peekKeyword())) {

            if (ts.peek().equals("end")) {
                ts.consume("end");
                elements.add(new PatternElement(null, List.of(), List.of(), Quantifier.ONE, Anchor.END));
                break;
            }

            if (ts.peek().equals("*")) {
                ts.consume("*");
                elements.add(new PatternElement(null, List.of(), List.of(), Quantifier.ZERO_MORE, null));
            } else {
                elements.add(parsePatternElement());
            }

            if (!ts.tryConsume(">>")) break;
        }

        return elements;
    }

    private PatternElement parsePatternElement() {
        // Optional tag name followed by (event1 | event2)
        // Forms: EventName, Tag(event), Tag(event1 | event2), Tag(^event)
        String tagName = null;
        List<String> names = new ArrayList<>();
        List<String> excluded = new ArrayList<>();

        // peek: if next token looks like identifier and THEN '(' — it's a tag
        String tok = ts.peek();
        if (ts.peekAhead(1).equals("(")) {
            tagName = ts.next();
            ts.consume("(");
            parseEventList(names, excluded);
            ts.consume(")");
        } else if (tok.equals("(")) {
            ts.consume("(");
            parseEventList(names, excluded);
            ts.consume(")");
        } else {
            // bare event name
            names.add(ts.next());
        }

        Quantifier q = parseQuantifier();
        return new PatternElement(tagName, List.copyOf(names), List.copyOf(excluded), q, null);
    }

    private void parseEventList(List<String> names, List<String> excluded) {
        do {
            boolean negate = ts.tryConsume("^");
            String name = ts.next();
            if (negate) excluded.add(name); else names.add(name);
        } while (ts.tryConsume("|") || ts.tryConsume(","));
    }

    private Quantifier parseQuantifier() {
        if (ts.tryConsume("+")) return Quantifier.ONE_MORE;
        if (ts.tryConsume("*")) return Quantifier.ZERO_MORE;
        if (ts.tryConsume("?")) return Quantifier.ZERO_ONE;
        if (ts.peek().equals("{")) {
            ts.consume("{");
            String inner = ts.nextUntil("}");
            ts.consume("}");
            return parseQuantifierBraces(inner.trim());
        }
        return Quantifier.ONE;
    }

    private Quantifier parseQuantifierBraces(String s) {
        if (s.contains(",")) {
            String[] parts = s.split(",", 2);
            int lo = parts[0].isBlank() ? 0 : Integer.parseInt(parts[0].trim());
            int hi = parts[1].isBlank() ? -1 : Integer.parseInt(parts[1].trim());
            return new Quantifier(lo, hi);
        }
        int n = Integer.parseInt(s);
        return new Quantifier(n, n);
    }

    // -----------------------------------------------------------------------
    // FILTER
    // -----------------------------------------------------------------------

    private SolOperation parseFilter() {
        ts.consume("filter");
        Expr condition = parseExpr();
        return new FilterOp(condition);
    }

    // -----------------------------------------------------------------------
    // SET
    // -----------------------------------------------------------------------

    private SolOperation parseSet() {
        ts.consume("set");
        // target = expression (target ends at '=')
        String target = ts.nextUntil("=").trim();
        ts.consume("=");
        Expr expr = parseExpr();
        return new SetOp(target, expr);
    }

    // -----------------------------------------------------------------------
    // REPLACE
    // -----------------------------------------------------------------------

    private SolOperation parseReplace() {
        ts.consume("replace");
        String tagName = ts.next();
        ts.consume("with");

        List<ReplacementElement> replacements = new ArrayList<>();
        if (ts.tryConsume("null")) {
            // empty = remove
        } else {
            replacements = parseReplacementPattern(tagName);
        }

        Map<String, Expr> dims = new LinkedHashMap<>();
        if (ts.tryConsume("dims")) {
            do {
                String dimTarget = ts.nextUntil("=").trim();
                ts.consume("=");
                Expr expr = parseExpr();
                dims.put(dimTarget, expr);
            } while (ts.tryConsume(","));
        }

        return new ReplaceOp(tagName, List.copyOf(replacements), Map.copyOf(dims));
    }

    private List<ReplacementElement> parseReplacementPattern(String defaultTagName) {
        List<ReplacementElement> elements = new ArrayList<>();
        do {
            String tok = ts.peek();
            if (ts.peekAhead(1).equals("(")) {
                String name = ts.next();
                ts.consume("(");
                String inner = ts.next();
                ts.consume(")");
                if (inner.startsWith("@")) {
                    elements.add(new ReplacementElement.CopyTag(name, inner.substring(1)));
                } else {
                    elements.add(new ReplacementElement.NewEvent(name, inner));
                }
            } else if (tok.equals("(")) {
                ts.consume("(");
                String inner = ts.next();
                ts.consume(")");
                elements.add(new ReplacementElement.NewEvent(null, inner));
            } else {
                // bare tag reference = copy
                String name = ts.next();
                elements.add(new ReplacementElement.CopyTag(name, name));
            }
        } while (ts.tryConsume(">>"));
        return elements;
    }

    // -----------------------------------------------------------------------
    // COMBINE
    // -----------------------------------------------------------------------

    private SolOperation parseCombine() {
        ts.consume("combine");
        Map<String, Expr> aggs = new LinkedHashMap<>();
        // optional: dim = expr, dim = expr, ...  (on same or next line, separated by comma)
        while (ts.hasMore() && !isOperationKeyword(ts.peekKeyword())) {
            String dimName = ts.nextUntil("=").trim();
            if (dimName.isEmpty()) break;
            ts.consume("=");
            Expr expr = parseExpr();
            aggs.put(dimName, expr);
            ts.tryConsume(",");
        }
        return new CombineOp(Map.copyOf(aggs));
    }

    // -----------------------------------------------------------------------
    // Expression parsing (precedence climbing)
    // -----------------------------------------------------------------------

    private Expr parseExpr() { return parseOr(); }

    private Expr parseOr() {
        Expr left = parseAnd();
        while (ts.tryConsume("or")) left = new Expr.BinaryOp("or", left, parseAnd());
        return left;
    }

    private Expr parseAnd() {
        Expr left = parseNot();
        while (ts.tryConsume("and")) left = new Expr.BinaryOp("and", left, parseNot());
        return left;
    }

    private Expr parseNot() {
        if (ts.tryConsume("not")) return new Expr.UnaryOp("not", parseNot());
        return parseComparison();
    }

    private Expr parseComparison() {
        Expr left = parseAddSub();
        for (String op : new String[]{"<=", ">=", "<", ">", "!=", "="}) {
            if (ts.tryConsume(op)) return new Expr.BinaryOp(op, left, parseAddSub());
        }
        if (ts.tryConsume("between")) {
            Expr lo = parseAddSub(); ts.consume("and"); Expr hi = parseAddSub();
            return new Expr.BetweenExpr(left, lo, hi);
        }
        if (ts.tryConsume("in")) return new Expr.InExpr(left, parseAtom());
        if (ts.tryConsume("like")) return new Expr.BinaryOp("like", left, parseAtom());
        if (ts.tryConsume("similar")) { ts.consume("to"); return new Expr.BinaryOp("similar to", left, parseAtom()); }
        return left;
    }

    private Expr parseAddSub() {
        Expr left = parseMulDiv();
        while (true) {
            if (ts.tryConsume("+")) left = new Expr.BinaryOp("+", left, parseMulDiv());
            else if (ts.tryConsume("-")) left = new Expr.BinaryOp("-", left, parseMulDiv());
            else break;
        }
        return left;
    }

    private Expr parseMulDiv() {
        Expr left = parsePow();
        while (true) {
            if (ts.tryConsume("*")) left = new Expr.BinaryOp("*", left, parsePow());
            else if (ts.tryConsume("/")) left = new Expr.BinaryOp("/", left, parsePow());
            else break;
        }
        return left;
    }

    private Expr parsePow() {
        Expr left = parseUnaryMinus();
        if (ts.tryConsume("^")) return new Expr.BinaryOp("^", left, parsePow());
        return left;
    }

    private Expr parseUnaryMinus() {
        if (ts.tryConsume("-")) return new Expr.UnaryOp("-", parseAtom());
        return parseAtom();
    }

    private Expr parseAtom() {
        String tok = ts.peek();

        // Parenthesised expression
        if (tok.equals("(")) {
            ts.consume("("); Expr e = parseExpr(); ts.consume(")"); return e;
        }

        // String literal
        if (tok.startsWith("'")) {
            return new Expr.Literal(ts.next().replaceAll("^'|'$", ""));
        }

        // Numeric literal — may be a combined duration token like "1h", "5min", "10s"
        if (tok.matches("-?\\d+(\\.\\d+)?[A-Za-z]*")) {
            // Try: combined token first (e.g. "1h", "30min", "5.5s")
            java.util.regex.Matcher dm = java.util.regex.Pattern
                    .compile("(-?\\d+(?:\\.\\d+)?)([A-Za-z]+)")
                    .matcher(tok);
            if (dm.matches()) {
                Duration dur = parseDurationSuffix(dm.group(1), dm.group(2));
                if (dur != null) { ts.next(); return new Expr.Literal(dur); }
            }
            // Pure number — then peek for a separate suffix token
            if (tok.matches("-?\\d+(\\.\\d+)?")) {
                String n = ts.next();
                String sfx = ts.peek();
                Duration dur = parseDurationSuffix(n, sfx);
                if (dur != null) { ts.next(); return new Expr.Literal(dur); }
                return n.contains(".") ? new Expr.Literal(Double.parseDouble(n))
                                       : new Expr.Literal(Long.parseLong(n));
            }
        }

        // Boolean
        if (tok.equalsIgnoreCase("true"))  { ts.next(); return new Expr.Literal(Boolean.TRUE); }
        if (tok.equalsIgnoreCase("false")) { ts.next(); return new Expr.Literal(Boolean.FALSE); }
        if (tok.equalsIgnoreCase("null"))  { ts.next(); return new Expr.Literal(null); }

        // MATCHED shorthand
        if (tok.equals("MATCHED")) { ts.next(); return new Expr.TagRef("MATCHED"); }

        // Function call or tag/dim reference
        if (tok.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            String name = ts.next();
            if (ts.peek().equals("(")) {
                // function call
                ts.consume("(");
                List<Expr> args = new ArrayList<>();
                if (!ts.peek().equals(")")) {
                    do { args.add(parseExpr()); } while (ts.tryConsume(","));
                }
                ts.consume(")");
                return new Expr.FunctionCall(name, List.copyOf(args));
            }

            // Tag[n].dim or Tag.dim or Tag[n:m].dim
            Integer sliceFrom = null, sliceTo = null;
            boolean hasSlice = false;
            if (ts.peek().equals("[")) {
                ts.consume("[");
                String inner = ts.nextUntil("]");
                ts.consume("]");
                hasSlice = true;
                if (inner.contains(":")) {
                    String[] parts = inner.split(":", 2);
                    sliceFrom = parts[0].isBlank() ? null : Integer.parseInt(parts[0].trim());
                    sliceTo   = parts[1].isBlank() ? null : Integer.parseInt(parts[1].trim());
                } else {
                    sliceFrom = Integer.parseInt(inner.trim());
                    sliceTo   = sliceFrom + 1;
                }
            }

            if (ts.peek().equals(".")) {
                ts.consume(".");
                String dim = ts.next();
                Integer idx = (hasSlice && Objects.equals(sliceTo, sliceFrom != null ? sliceFrom + 1 : null))
                        ? sliceFrom : null;
                return new Expr.DimRef(name, idx, dim);
            }

            if (hasSlice) {
                // array slice on tag
                return new Expr.ArraySlice(new Expr.TagRef(name), sliceFrom, sliceTo);
            }

            return new Expr.TagRef(name);
        }

        throw new SolParseException("Unexpected token in expression: '" + tok + "'");
    }

    // -----------------------------------------------------------------------
    // Duration suffix parsing
    // -----------------------------------------------------------------------

    private static Duration parseDurationSuffix(String number, String suffix) {
        double v = Double.parseDouble(number);
        return switch (suffix) {
            case "ms"  -> Duration.ofMillis((long) v);
            case "s"   -> Duration.ofMillis((long)(v * 1_000));
            case "min" -> Duration.ofMillis((long)(v * 60_000));
            case "h"   -> Duration.ofMillis((long)(v * 3_600_000));
            case "d"   -> Duration.ofMillis((long)(v * 86_400_000));
            case "w"   -> Duration.ofMillis((long)(v * 604_800_000));
            default    -> null;
        };
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static final Set<String> OPERATION_KEYWORDS =
            Set.of("match", "filter", "set", "replace", "combine");

    private static boolean isOperationKeyword(String kw) {
        return OPERATION_KEYWORDS.contains(kw);
    }
}
