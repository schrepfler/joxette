package com.sol.expr;

import java.time.Duration;
import java.util.List;

/**
 * AST node for a SOL expression. Every node is immutable and produced by
 * {@link com.sol.parser.SolParser}.
 */
public sealed interface Expr
        permits Expr.Literal,
                Expr.DimRef,
                Expr.TagRef,
                Expr.BinaryOp,
                Expr.UnaryOp,
                Expr.FunctionCall,
                Expr.ArraySlice,
                Expr.InExpr,
                Expr.BetweenExpr,
                Expr.IfExpr {

    // -----------------------------------------------------------------------
    // Leaf nodes
    // -----------------------------------------------------------------------

    /** A literal constant: String, Long, Double, Boolean, Duration, or null. */
    record Literal(Object value) implements Expr {}

    /**
     * Reference to a dimension on a tag, a specific event, or the sequence.
     *
     * <p>Examples: {@code event1.ts}, {@code SEQ.amount}, {@code Tag[0].name}
     *
     * @param tagName   tag or "SEQ"
     * @param index     null = broadcast over whole tag; otherwise Python-style index
     * @param dimName   dimension name (null = the whole tag/event object)
     */
    record DimRef(String tagName, Integer index, String dimName) implements Expr {}

    /**
     * Reference to a tag used as a boolean (MATCHED shorthand) or array.
     */
    record TagRef(String tagName) implements Expr {}

    // -----------------------------------------------------------------------
    // Composite nodes
    // -----------------------------------------------------------------------

    record BinaryOp(String op, Expr left, Expr right) implements Expr {}

    record UnaryOp(String op, Expr operand) implements Expr {}

    /**
     * A built-in function call: {@code duration(a, b)}, {@code sum(Tag.dim)}, etc.
     */
    record FunctionCall(String name, List<Expr> args) implements Expr {}

    /**
     * Array/tag slicing: {@code Tag[n:m]}, {@code Tag[n]}, {@code Tag[-1]}.
     *
     * @param base  expression that evaluates to an array or tag
     * @param from  null = start
     * @param to    null = end
     */
    record ArraySlice(Expr base, Integer from, Integer to) implements Expr {}

    /** {@code value IN array} */
    record InExpr(Expr value, Expr array) implements Expr {}

    /** {@code value BETWEEN lo AND hi} */
    record BetweenExpr(Expr value, Expr lo, Expr hi) implements Expr {}

    /** {@code IF(cond, thenExpr, elseExpr)} */
    record IfExpr(Expr condition, Expr thenExpr, Expr elseExpr) implements Expr {}
}
