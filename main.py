from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict
from uuid import uuid4
from eventsourcing.application.sqlalchemy import SQLAlchemyApplication
from eventsourcing.domain.model.aggregate import AggregateRoot

Column = int


@dataclass
class Override:
    column: int
    value: int


Overrides = Dict[Column, Override]


@dataclass
class Datasource:
    id: str
    name: str


@dataclass
class Row:
    id: int
    datasource_id: str
    overrides: Overrides


class Report(AggregateRoot):
    def __init__(self, name, **kwargs):
        super(Report, self).__init__(**kwargs)
        self.name = name
        self.rows = OrderedDict()

    def add_row(self, datasource_id=None):
        self.__trigger_event__(
            Report.RowAdded, id=str(uuid4()), datasource_id=datasource_id, overrides={}
        )

    def override_value(self, row_id, column, value):
        # raise exception if row id does not exist
        if row_id is None:
            raise Exception("Missing row id")

        self.__trigger_event__(
            Report.ValueOverridden, row_id=row_id, column=column, value=value
        )

    class RowAdded(AggregateRoot.Event):
        @property
        def datasource_id(self):
            return self.__dict__["datasource_id"]

        @property
        def overrides(self):
            return self.__dict__["overrides"]

        def mutate(self, obj):
            obj.rows[self.id] = Row(
                id=self.id, datasource_id=self.datasource_id, overrides=self.overrides
            )

    class ValueOverridden(AggregateRoot.Event):
        @property
        def row_id(self):
            return self.__dict__["row_id"]

        @property
        def column(self):
            return self.__dict__["column"]

        @property
        def value(self):
            return self.__dict__["value"]

        def mutate(self, obj):
            obj.rows[self.row_id].overrides[self.column] = {"value": str(self.value)}


if __name__ == "__main__":
    with SQLAlchemyApplication(persist_event_type=Report.Event) as app:
        report = Report.__create__(name="my report")
        report.add_row(datasource_id="e1b2b779-192f-40a4-9dc1-46348e40fc95")

        rows = list(report.rows.values())
        first_row_id = rows[0].id
        report.override_value(row_id=first_row_id, column=3, value="1500")

        report.__save__()
        assert report.id in app.repository
