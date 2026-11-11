"""Wire messages for the Job Overview protocol. See PROTOCOL.md in the repository root."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator
from pydantic.alias_generators import to_camel

PROTOCOL_VERSION = 1

Progress = Annotated[float, Field(ge=0.0, le=1.0)]
JobId = Annotated[str, Field(min_length=1, max_length=200)]


class _Message(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="ignore")


class Hello(_Message):
    type: Literal["hello"]
    role: Literal["producer", "widget"]
    protocol: int = PROTOCOL_VERSION
    client_id: str | None = None


# --- producer -> server -------------------------------------------------------


class NewJobSpec(_Message):
    id: JobId
    title: str
    description: str = ""
    progress: Progress | None = None
    stale_after: Annotated[float, Field(gt=0)] | None = None


class NewJob(_Message):
    type: Literal["newJob"]
    job: NewJobSpec


class UpdateJob(_Message):
    type: Literal["updateJob"]
    id: JobId
    title: str | None = None
    description: str | None = None
    # An explicit `null` clears the progress bar; leaving the field out keeps it.
    progress: Progress | None = None

    @model_validator(mode="after")
    def _at_least_one_change(self) -> UpdateJob:
        if not {"title", "description", "progress"} & self.model_fields_set:
            raise ValueError("updateJob needs at least one of title, description or progress")
        return self


class Heartbeat(_Message):
    type: Literal["heartbeat"]
    id: JobId


class EndJob(_Message):
    type: Literal["endJob"]
    id: JobId


class EndAllJobs(_Message):
    type: Literal["endAllJobs"]


ProducerMessage = Annotated[NewJob | UpdateJob | Heartbeat | EndJob | EndAllJobs, Field(discriminator="type")]
producer_message: TypeAdapter[ProducerMessage] = TypeAdapter(ProducerMessage)


# --- widget -> server ---------------------------------------------------------


class DismissJob(_Message):
    type: Literal["dismissJob"]
    id: JobId


class Resync(_Message):
    type: Literal["resync"]


WidgetMessage = Annotated[DismissJob | Resync, Field(discriminator="type")]
widget_message: TypeAdapter[WidgetMessage] = TypeAdapter(WidgetMessage)
