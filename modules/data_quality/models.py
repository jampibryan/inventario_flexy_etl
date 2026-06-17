from dataclasses import dataclass, field
from typing import Literal

import pandas as pd


Severity = Literal["warning", "error"]


@dataclass(frozen=True)
class DataQualityIssue:
    rule_id: str
    status_code: str
    severity: Severity
    message: str


@dataclass
class DataQualityResult:
    dataframe: pd.DataFrame | None = None
    file_date: str = ""
    issues: list[DataQualityIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(issue.severity == "error" for issue in self.issues)

    @property
    def blocking_issue(self) -> DataQualityIssue | None:
        return next((issue for issue in self.issues if issue.severity == "error"), None)

    @property
    def warning_messages(self) -> list[str]:
        return [issue.message for issue in self.issues if issue.severity == "warning"]
