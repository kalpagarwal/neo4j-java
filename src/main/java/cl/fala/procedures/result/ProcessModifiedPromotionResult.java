package cl.fala.procedures.result;

public class ProcessModifiedPromotionResult {
    public String offeringId;
    public String type;
    public String action;

    public ProcessModifiedPromotionResult(String offeringId, String type, String action) {
        this.offeringId = offeringId;
        this.type = type;
        this.action = action;
    }
}
