package cl.fala.procedures.pojo;

public class Operator {
    public enum OPERATOR {
        AND,
        OR
    }

    private final OPERATOR operator;

    public Operator(String operator) {
        if (operator.equals("AND")) {
            this.operator = OPERATOR.AND;
        } else {
            this.operator = OPERATOR.OR;
        }
    }

    public OPERATOR getOperator() {
        return operator;
    }
}
