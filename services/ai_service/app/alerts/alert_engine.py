class AlertEngine:
    """
    Converts AI risk scores into actionable alerts
    """

    def __init__(self, threshold=0.8):
        self.threshold = threshold

    def evaluate(self, cow_id: int, risk_score: float):
        """
        Decide if cow needs attention
        """

        if risk_score >= self.threshold:
            return {
                "cow_id": cow_id,
                "alert_level": "CRITICAL",
                "message": "Possible illness detected",
                "action": "Notify vet immediately"
            }

        elif risk_score >= 0.5:
            return {
                "cow_id": cow_id,
                "alert_level": "WARNING",
                "message": "Behavior deviation detected",
                "action": "Monitor closely"
            }

        else:
            return {
                "cow_id": cow_id,
                "alert_level": "NORMAL",
                "message": "All good",
                "action": "No action"
            }