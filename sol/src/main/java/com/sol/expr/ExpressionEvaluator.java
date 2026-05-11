package com.sol.expr;

import com.sol.model.Event;
import com.sol.model.Tag;

import java.time.Duration;
import java.time.Instant;
import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Evaluates a SOL {@link Expr} against an {@link EvalContext}.
 *
 * <p>Follows the "show must go on" principle: uncomputable sub-expressions
 * produce {@code null} and are recorded as unexpected nulls in the context
 * rather than throwing.
 */
public final class ExpressionEvaluator {

    private ExpressionEvaluator() {}

    // Sealed switch — exhaustive by compiler.
    public static Object eval(Expr expr, EvalContext ctx) {
        return switch (expr) {
            case Expr.Literal(var v)                         -> v;
            case Expr.TagRef(var name)                       -> evalTagRef(name, ctx);
            case Expr.DimRef(var tag, var idx, var dim)      -> evalDimRef(tag, idx, dim, ctx);
            case Expr.BinaryOp(var op, var l, var r)         -> evalBinary(op, l, r, ctx);
            case Expr.UnaryOp(var op, var operand)           -> evalUnary(op, operand, ctx);
            case Expr.FunctionCall(var name, var args)       -> evalFunction(name, args, ctx);
            case Expr.ArraySlice(var base, var from, var to) -> evalSlice(base, from, to, ctx);
            case Expr.InExpr(var val, var arr)               -> evalIn(val, arr, ctx);
            case Expr.BetweenExpr(var val, var lo, var hi)  -> evalBetween(val, lo, hi, ctx);
            case Expr.IfExpr(var cond, var t, var e)         -> evalIf(cond, t, e, ctx);
        };
    }

    public static boolean evalBoolean(Expr expr, EvalContext ctx) {
        return switch (eval(expr, ctx)) {
            case Boolean b -> b;
            case null      -> false;
            case Object v  -> ctx.recordNull("boolean coercion",
                                    "expected Boolean, got " + v.getClass().getSimpleName()) != null;
        };
    }

    // -----------------------------------------------------------------------
    // Tag / dim resolution
    // -----------------------------------------------------------------------

    private static Object evalTagRef(String name, EvalContext ctx) {
        if ("MATCHED".equals(name)) {
            Tag m = ctx.tag("MATCHED");
            return m != null && !m.isEmpty();
        }
        Tag t = ctx.tag(name);
        return t != null ? t : ctx.recordNull("tag ref '" + name + "'", "tag not defined");
    }

    private static Object evalDimRef(String tagName, Integer idx, String dimName, EvalContext ctx) {
        if ("SEQ".equals(tagName) && idx == null) {
            if (dimName == null) return ctx.sequence().events();
            Object seqDim = ctx.sequence().dim(dimName);
            return seqDim != null ? seqDim : broadcast(ctx.sequence().events(), dimName);
        }

        Tag tag = ctx.tag(tagName);
        if (tag == null)
            return ctx.recordNull("dim ref " + tagName + "." + dimName,
                                  "tag '" + tagName + "' not defined");

        if (idx != null) {
            int abs = tag.resolveIndex(idx);
            if (abs < 0)
                return ctx.recordNull("dim ref " + tagName + "[" + idx + "]." + dimName,
                                      "index out of range");
            Event ev = ctx.sequence().get(abs);
            return dimName == null ? ev : ev.dim(dimName);
        }

        List<Event> events = tag.events(ctx.sequence());
        return dimName == null ? events : broadcast(events, dimName);
    }

    /**
     * Returns a lazy view of a dimension across all events in the list.
     * No copying — the view reads {@code Event.dim(dimName)} on each access,
     * so callers like {@code fnAny}/{@code fnAll}/{@code evalIn} can short-circuit
     * without materialising the full array.
     */
    private static List<Object> broadcast(List<Event> events, String dimName) {
        return new AbstractList<>() {
            @Override public Object get(int index) { return events.get(index).dim(dimName); }
            @Override public int size()            { return events.size(); }
        };
    }

    // -----------------------------------------------------------------------
    // Binary operators
    // -----------------------------------------------------------------------

    private static Object evalBinary(String op, Expr left, Expr right, EvalContext ctx) {
        return switch (op) {
            case "and" -> {
                Boolean l = asBoolean(eval(left, ctx));
                if (Boolean.FALSE.equals(l)) yield false;
                Boolean r = asBoolean(eval(right, ctx));
                yield l != null && r != null ? l && r : null;
            }
            case "or" -> {
                Boolean l = asBoolean(eval(left, ctx));
                if (Boolean.TRUE.equals(l)) yield true;
                Boolean r = asBoolean(eval(right, ctx));
                yield l != null && r != null ? l || r : null;
            }
            case "=" -> {
                Object l = eval(left, ctx), r = eval(right, ctx);
                yield l == null || r == null ? null : Objects.equals(l, r);
            }
            case "!=" -> {
                Object l = eval(left, ctx), r = eval(right, ctx);
                yield l == null || r == null ? null : !Objects.equals(l, r);
            }
            case "<", ">", "<=", ">=" -> compare(op, eval(left, ctx), eval(right, ctx), ctx);
            case "+", "-", "*", "/", "^" -> arithmetic(op, eval(left, ctx), eval(right, ctx), ctx);
            case "like"       -> evalLike(eval(left, ctx), eval(right, ctx), ctx);
            case "similar to" -> evalSimilar(eval(left, ctx), eval(right, ctx), ctx);
            default -> ctx.recordNull("binary op '" + op + "'", "unknown operator");
        };
    }

    private static Object compare(String op, Object l, Object r, EvalContext ctx) {
        if (l == null || r == null) return null;
        return switch (l) {
            case Comparable<?> lc when r instanceof Comparable<?> -> {
                @SuppressWarnings("unchecked")
                int cmp = ((Comparable<Object>) lc).compareTo(r);
                yield switch (op) {
                    case "<"  -> cmp < 0;
                    case ">"  -> cmp > 0;
                    case "<=" -> cmp <= 0;
                    case ">=" -> cmp >= 0;
                    default   -> null;
                };
            }
            default -> ctx.recordNull("compare '" + op + "'",
                                      "non-comparable types: " + l.getClass().getSimpleName());
        };
    }

    private static Object arithmetic(String op, Object l, Object r, EvalContext ctx) {
        if (l == null || r == null) return null;
        // Temporal arithmetic resolved before numeric fallthrough
        if (l instanceof Instant li) {
            if (op.equals("-") && r instanceof Instant ri) return Duration.between(ri, li);
            if (op.equals("+") && r instanceof Duration d)  return li.plus(d);
            if (op.equals("-") && r instanceof Duration d)  return li.minus(d);
        }
        return switch (op) {
            case "+" -> toDouble(l) + toDouble(r);
            case "-" -> toDouble(l) - toDouble(r);
            case "*" -> toDouble(l) * toDouble(r);
            case "/" -> {
                double rd = toDouble(r);
                yield rd == 0 ? ctx.recordNull("division", "division by zero") : toDouble(l) / rd;
            }
            case "^" -> Math.pow(toDouble(l), toDouble(r));
            default  -> ctx.recordNull("arithmetic '" + op + "'", "unknown operator");
        };
    }

    private static Object evalLike(Object val, Object pat, EvalContext ctx) {
        return switch (val) {
            case String s when pat instanceof String p ->
                    s.matches("^" + p.replace("%", ".*").replace("_", ".") + "$");
            default -> ctx.recordNull("like", "non-string operands");
        };
    }

    private static Object evalSimilar(Object val, Object pat, EvalContext ctx) {
        return switch (val) {
            case String s when pat instanceof String p -> Pattern.compile(p).matcher(s).find();
            default -> ctx.recordNull("similar to", "non-string operands");
        };
    }

    // -----------------------------------------------------------------------
    // Unary
    // -----------------------------------------------------------------------

    private static Object evalUnary(String op, Expr operand, EvalContext ctx) {
        Object v = eval(operand, ctx);
        return switch (op) {
            case "not" -> switch (v) {
                case Boolean b -> !b;
                default        -> ctx.recordNull("not", "non-boolean operand");
            };
            case "-" -> switch (v) {
                case Number n -> -n.doubleValue();
                default       -> ctx.recordNull("negate", "non-numeric operand");
            };
            default -> ctx.recordNull("unary '" + op + "'", "unknown operator");
        };
    }

    // -----------------------------------------------------------------------
    // Functions
    // -----------------------------------------------------------------------

    private static Object evalFunction(String name, List<Expr> args, EvalContext ctx) {
        // Short-circuit functions: evaluate only the arguments they actually need.
        return switch (name.toLowerCase()) {
            case "if" -> {
                if (args.size() != 3) yield ctx.recordNull("if()", "expected 3 arguments");
                yield Boolean.TRUE.equals(asBoolean(eval(args.get(0), ctx)))
                        ? eval(args.get(1), ctx)
                        : eval(args.get(2), ctx);
            }
            case "coalesce" -> {
                for (Expr arg : args) {
                    Object v = eval(arg, ctx);
                    if (v != null) yield v;
                }
                yield null;
            }
            // All other functions: eager evaluation is fine, materialise once.
            default -> {
                List<Object> evaled = args.stream().map(a -> eval(a, ctx)).toList();
                yield switch (name.toLowerCase()) {
                    case "duration" -> fnDuration(evaled, ctx);
                    case "length"   -> fnLength(evaled, ctx);
                    case "sum"      -> fnSum(evaled, ctx);
                    case "min"      -> fnMin(evaled, ctx);
                    case "max"      -> fnMax(evaled, ctx);
                    case "avg"      -> fnAvg(evaled, ctx);
                    case "any"      -> fnAny(evaled, ctx);
                    case "all"      -> fnAll(evaled, ctx);
                    case "unique"   -> fnUnique(evaled, ctx);
                    case "position" -> fnPosition(evaled, ctx);
                    case "concat"   -> fnConcat(evaled);
                    case "lower"    -> fnStr(evaled, ctx, String::toLowerCase);
                    case "upper"    -> fnStr(evaled, ctx, String::toUpperCase);
                    case "strlen"   -> fnStr(evaled, ctx, s -> (Object)(long) s.length());
                    case "now"      -> Instant.now();
                    case "abs"      -> fnAbs(evaled, ctx);
                    case "round"    -> fnMath(evaled, ctx, Math::rint);
                    case "floor"    -> fnMath(evaled, ctx, Math::floor);
                    case "ceiling"  -> fnMath(evaled, ctx, Math::ceil);
                    case "log"      -> fnMath(evaled, ctx, Math::log10);
                    default         -> ctx.recordNull("function '" + name + "'", "unknown function");
                };
            }
        };
    }

    private static Object fnDuration(List<Object> args, EvalContext ctx) {
        if (args.size() != 2) return ctx.recordNull("duration()", "expected 2 arguments");
        Instant t1 = toInstant(args.getFirst(), ctx);
        Instant t2 = toInstant(args.getLast(), ctx);
        return t1 == null || t2 == null ? null : Duration.between(t1, t2);
    }

    private static Object fnLength(List<Object> args, EvalContext ctx) {
        if (args.isEmpty()) return null;
        return switch (args.getFirst()) {
            case List<?> l -> (long) l.size();
            case Tag t     -> (long) t.length();
            case null      -> ctx.recordNull("length()", "null argument");
            default        -> ctx.recordNull("length()", "expected array or tag");
        };
    }

    private static Object fnSum(List<Object> args, EvalContext ctx) {
        List<?> arr = toList(args, ctx, "sum()");
        return arr == null ? null : arr.stream().filter(Objects::nonNull).mapToDouble(ExpressionEvaluator::toDouble).sum();
    }

    private static Object fnMin(List<Object> args, EvalContext ctx) {
        List<?> arr = toList(args, ctx, "min()");
        return arr == null ? null : arr.stream().filter(Objects::nonNull).mapToDouble(ExpressionEvaluator::toDouble).min().orElse(Double.NaN);
    }

    private static Object fnMax(List<Object> args, EvalContext ctx) {
        List<?> arr = toList(args, ctx, "max()");
        return arr == null ? null : arr.stream().filter(Objects::nonNull).mapToDouble(ExpressionEvaluator::toDouble).max().orElse(Double.NaN);
    }

    private static Object fnAvg(List<Object> args, EvalContext ctx) {
        List<?> arr = toList(args, ctx, "avg()");
        return arr == null ? null : arr.stream().mapToDouble(v -> v == null ? 0.0 : toDouble(v)).average().orElse(Double.NaN);
    }

    private static Object fnAny(List<Object> args, EvalContext ctx) {
        List<?> arr = toList(args, ctx, "any()");
        return arr == null ? null : arr.stream().anyMatch(Objects::nonNull);
    }

    private static Object fnAll(List<Object> args, EvalContext ctx) {
        List<?> arr = toList(args, ctx, "all()");
        return arr == null ? null : arr.stream().allMatch(Objects::nonNull);
    }

    private static Object fnUnique(List<Object> args, EvalContext ctx) {
        List<?> arr = toList(args, ctx, "unique()");
        return arr == null ? null : arr.stream().distinct().toList();
    }

    private static Object fnPosition(List<Object> args, EvalContext ctx) {
        if (args.isEmpty()) return null;
        return switch (args.getFirst()) {
            case Tag t -> (long) t.from();
            default    -> ctx.recordNull("position()", "expected a tag argument");
        };
    }

    private static Object fnConcat(List<Object> args) {
        StringBuilder sb = new StringBuilder();
        for (Object a : args) sb.append(a == null ? "" : a.toString());
        return sb.toString();
    }

    private static Object fnStr(List<Object> args, EvalContext ctx,
                                 java.util.function.Function<String, Object> fn) {
        if (args.isEmpty()) return null;
        return switch (args.getFirst()) {
            case String s -> fn.apply(s);
            default       -> ctx.recordNull("string fn", "non-string argument");
        };
    }

    private static Object fnAbs(List<Object> args, EvalContext ctx) {
        if (args.isEmpty()) return null;
        return switch (args.getFirst()) {
            case List<?> l -> l.stream().map(e -> e == null ? null : Math.abs(toDouble(e))).toList();
            case Object v  -> Math.abs(toDouble(v));
        };
    }

    private static Object fnMath(List<Object> args, EvalContext ctx,
                                  java.util.function.DoubleUnaryOperator fn) {
        if (args.isEmpty()) return null;
        return fn.applyAsDouble(toDouble(args.getFirst()));
    }

    // -----------------------------------------------------------------------
    // Array slice
    // -----------------------------------------------------------------------

    private static Object evalSlice(Expr base, Integer from, Integer to, EvalContext ctx) {
        Object v = eval(base, ctx);
        List<Object> list = switch (v) {
            case null      -> null;
            case List<?> l -> {
                @SuppressWarnings("unchecked") var cast = (List<Object>) l;
                yield cast;
            }
            case Tag t -> {
                @SuppressWarnings("unchecked") var cast = (List<Object>) (List<?>) t.events(ctx.sequence());
                yield cast;
            }
            default -> {
                ctx.recordNull("slice", "expected array, got " + v.getClass().getSimpleName());
                yield null;
            }
        };
        if (list == null) return null;
        int size = list.size();
        int f = from == null ? 0    : (from < 0 ? Math.max(0, size + from) : Math.min(from, size));
        int t = to   == null ? size : (to   < 0 ? Math.max(0, size + to)   : Math.min(to,   size));
        return list.subList(f, Math.max(f, t));
    }

    // -----------------------------------------------------------------------
    // IN / BETWEEN / IF
    // -----------------------------------------------------------------------

    private static Object evalIn(Expr valueExpr, Expr arrayExpr, EvalContext ctx) {
        Object val = eval(valueExpr, ctx);
        Object arr = eval(arrayExpr, ctx);
        if (val == null || arr == null) return null;
        return switch (arr) {
            case List<?> l -> l.contains(val);
            default        -> ctx.recordNull("in", "right-hand side is not an array");
        };
    }

    private static Object evalBetween(Expr valueExpr, Expr loExpr, Expr hiExpr, EvalContext ctx) {
        Object val = eval(valueExpr, ctx), lo = eval(loExpr, ctx), hi = eval(hiExpr, ctx);
        if (val == null || lo == null || hi == null) return null;
        Boolean geLo = (Boolean) compare(">=", val, lo, ctx);
        Boolean leHi = (Boolean) compare("<=", val, hi, ctx);
        return geLo == null || leHi == null ? null : geLo && leHi;
    }

    private static Object evalIf(Expr cond, Expr thenExpr, Expr elseExpr, EvalContext ctx) {
        return Boolean.TRUE.equals(asBoolean(eval(cond, ctx)))
                ? eval(thenExpr, ctx) : eval(elseExpr, ctx);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Boolean asBoolean(Object v) {
        return switch (v) {
            case Boolean b -> b;
            case null      -> null;
            default        -> null;
        };
    }

    /** Converts a runtime value to double for numeric arithmetic. */
    private static double toDouble(Object v) {
        return switch (v) {
            case Number n   -> n.doubleValue();
            case Duration d -> (double) d.toMillis();
            case null       -> 0.0;
            default         -> 0.0;
        };
    }

    private static Instant toInstant(Object v, EvalContext ctx) {
        return switch (v) {
            case Instant i               -> i;
            case Event e                 -> e.ts();
            case Tag t when !t.isEmpty() -> ctx.sequence().get(t.from()).ts();
            case null                    -> null;
            default                      -> null;
        };
    }

    private static List<?> toList(List<Object> args, EvalContext ctx, String fn) {
        if (args.isEmpty()) { ctx.recordNull(fn, "no argument"); return null; }
        return switch (args.getFirst()) {
            case List<?> l -> l;
            case null      -> { ctx.recordNull(fn, "null argument"); yield null; }
            default        -> { ctx.recordNull(fn, "expected array"); yield null; }
        };
    }
}
