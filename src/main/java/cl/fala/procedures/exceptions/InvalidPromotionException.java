package cl.fala.procedures.exceptions;

public class InvalidPromotionException extends Exception {
    public InvalidPromotionException(String message) {
        super(Messages.PromotionExceptions.BASE + message);
    }
}
