from enum import Enum

class AssessmentType(str, Enum):
    SCREENING = "screening"          # Quick external assessment
    DUE_DILIGENCE = "due_diligence"  # Deep dive with internal access
    QUARTERLY = "quarterly"          # Regular portfolio monitoring
    EXIT_PREP = "exit_prep"          # Pre-exit assessment

class AssessmentStatus(str, Enum):
    DRAFT = "draft"
    IN_PROGRESS = "in_progress"
    SUBMITTED = "submitted"
    APPROVED = "approved"
    SUPERSEDED = "superseded"

class Dimension(str, Enum):
    DATA_INFRASTRUCTURE = "data_infrastructure"
    AI_GOVERNANCE = "ai_governance"
    TECHNOLOGY_STACK = "technology_stack"
    TALENT_SKILLS = "talent_skills"
    LEADERSHIP_VISION = "leadership_vision"
    USE_CASE_PORTFOLIO = "use_case_portfolio"
    CULTURE_CHANGE = "culture_change"