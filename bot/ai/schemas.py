"""Response schemas for structured Gemini output.

Constraints (ranges, id membership) are enforced in code, not in the schema:
a schema violation fails the whole response, a bad value only drops one item.
"""

from pydantic import BaseModel, Field


class AuditFinding(BaseModel):
    message_id: int = Field(description="Номер сообщения из квадратных скобок")
    article: str = Field(description="Статья, например «ст. 20.3.1 КоАП РФ»")
    fines: int = Field(description="Штраф в рублях")
    days: int = Field(description="Срок в днях, 0 если не предусмотрен")


class AuditResult(BaseModel):
    violations: list[AuditFinding] = Field(default_factory=list)


class SummaryParticipant(BaseModel):
    name: str
    characterization: str


class SummaryResult(BaseModel):
    topic: str
    participants: list[SummaryParticipant] = Field(default_factory=list)
