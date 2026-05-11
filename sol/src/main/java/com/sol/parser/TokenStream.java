package com.sol.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple token stream that splits SOL source text into tokens.
 *
 * <p>Tokens are:
 * <ul>
 *   <li>Quoted strings {@code 'value'}</li>
 *   <li>Multi-char operators: {@code >>}, {@code <=}, {@code >=}, {@code !=}</li>
 *   <li>Single-char operators / punctuation: {@code ( ) [ ] { } , . ^ * + ? / = < > -}</li>
 *   <li>Identifiers / keywords / numbers (anything else non-whitespace)</li>
 * </ul>
 *
 * <p>Comments (// to end of line) and blank lines are stripped.
 */
final class TokenStream {

    private static final Pattern TOKEN_PATTERN = Pattern.compile(
            "'[^']*'"           // quoted string
            + "|>>"             // consecutive op
            + "|<="             // le
            + "|>="             // ge
            + "|!="             // ne
            + "|[()\\[\\]{},\\.^*+?/=<>\\-|]"  // single-char punctuation
            + "|[^\\s()\\[\\]{},\\.^*+?/=<>\\-|]+"  // word token
    );

    private final List<String> tokens;
    private int pos;

    TokenStream(String source) {
        tokens = tokenize(source);
        pos = 0;
    }

    private static List<String> tokenize(String source) {
        // Strip line comments
        String clean = source.replaceAll("//[^\n]*", " ").replaceAll("\\s+", " ").trim();
        List<String> result = new ArrayList<>();
        Matcher m = TOKEN_PATTERN.matcher(clean);
        while (m.find()) result.add(m.group());
        return result;
    }

    boolean hasMore() { return pos < tokens.size(); }

    String peek() { return hasMore() ? tokens.get(pos) : ""; }

    String peekAhead(int offset) {
        int i = pos + offset;
        return i < tokens.size() ? tokens.get(i) : "";
    }

    /** Returns the lowercased first word of the current position (for keyword dispatch). */
    String peekKeyword() { return peek().toLowerCase(); }

    String next() {
        if (!hasMore()) throw new SolParseException("Unexpected end of input");
        return tokens.get(pos++);
    }

    void consume(String expected) {
        String tok = next();
        if (!tok.equals(expected))
            throw new SolParseException("Expected '" + expected + "' but got '" + tok + "'");
    }

    boolean tryConsume(String expected) {
        if (peek().equals(expected)) { pos++; return true; }
        return false;
    }

    /** Consumes tokens concatenating them until the given delimiter token is next. */
    String nextUntil(String delimiter) {
        StringBuilder sb = new StringBuilder();
        while (hasMore() && !peek().equals(delimiter)) {
            if (!sb.isEmpty()) sb.append(' ');
            sb.append(next());
        }
        return sb.toString();
    }
}
