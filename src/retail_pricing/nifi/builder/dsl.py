"""A small DSL that writes NiFi 2.12 flow-definition JSON (format pinned by DECISIONS 97).

Ids are uuid5 of the component path, positions come from a grid in creation order and every
type, bundle and property descriptor comes from `nifi/definitions/*.json`, so the output is
deterministic and only uses property keys NiFi knows.
"""

import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

NAMESPACE = uuid.UUID("5b0f6c1e-8e43-4c55-9a43-6a3f1c6d2b10")
DEFINITIONS = Path("nifi/definitions")
GRID_COLUMNS = 6


def uid(path: str) -> str:
    return str(uuid.uuid5(NAMESPACE, path))


def load_definitions(directory: Path = DEFINITIONS) -> dict[str, dict[str, Any]]:
    """Definitions by simple type name, e.g. `PutSQL`."""
    return {
        p.stem: json.loads(p.read_text(encoding="utf-8")) for p in sorted(directory.glob("*.json"))
    }


@dataclass(frozen=True)
class Ref:
    """A connectable: processor or port."""

    id: str
    group: str
    kind: str  # PROCESSOR, INPUT_PORT or OUTPUT_PORT
    name: str


@dataclass
class Group:
    defs: dict[str, dict[str, Any]]
    name: str
    path: str
    context: str
    parent: str | None = None
    barrier: bool = False
    _n: int = 0
    procs: list[dict[str, Any]] = field(default_factory=list)
    services: list[dict[str, Any]] = field(default_factory=list)
    conns: list[dict[str, Any]] = field(default_factory=list)
    inputs: list[dict[str, Any]] = field(default_factory=list)
    outputs: list[dict[str, Any]] = field(default_factory=list)
    groups: list["Group"] = field(default_factory=list)

    @property
    def id(self) -> str:
        return uid(self.path)

    def _pos(self) -> dict[str, float]:
        n, self._n = self._n, self._n + 1
        return {"x": float(n % GRID_COLUMNS * 420), "y": float(n // GRID_COLUMNS * 220)}

    def _component(self, simple: str, name: str, props: dict[str, str]) -> dict[str, Any]:
        d = self.defs[simple]
        known = d.get("propertyDescriptors") or {}
        descriptors = {}
        for key in props:
            p = known.get(key)
            descriptors[key] = {
                "displayName": p["displayName"] if p else key,
                "dynamic": p is None,
                "identifiesControllerService": bool(p and p.get("typeProvidedByValue")),
                "name": key,
                "sensitive": bool(p and p.get("sensitive")),
            }
        return {
            "bundle": {"artifact": d["artifact"], "group": d["group"], "version": d["version"]},
            "comments": "",
            "groupIdentifier": self.id,
            "identifier": uid(f"{self.path}/{name}"),
            "name": name,
            "properties": dict(props),
            "propertyDescriptors": descriptors,
            "type": d["type"],
        }

    def _unique(self, name: str) -> None:
        taken = {c["name"] for c in self.procs + self.inputs + self.outputs + self.services}
        taken |= {g.name for g in self.groups}
        if name in taken:
            raise ValueError(f"{self.path}: duplicate component name {name!r}")

    def service(self, simple: str, name: str, props: dict[str, str]) -> str:
        self._unique(name)
        c = self._component(simple, name, props)
        c |= {
            "bulletinLevel": "WARN",
            "componentType": "CONTROLLER_SERVICE",
            "controllerServiceApis": [
                {
                    "bundle": {k: api[k] for k in ("artifact", "group", "version")},
                    "type": api["type"],
                }
                for api in self.defs[simple].get("providedApiImplementations", [])
            ],
            "scheduledState": "DISABLED",
        }
        self.services.append(c)
        return str(c["identifier"])

    def proc(
        self,
        simple: str,
        name: str,
        props: dict[str, str] | None = None,
        *,
        auto: tuple[str, ...] = (),
        tasks: int = 1,
        period: str = "0 sec",
        penalty: str = "30 sec",
    ) -> Ref:
        self._unique(name)
        c = self._component(simple, name, props or {})
        c |= {
            "autoTerminatedRelationships": sorted(auto),
            "backoffMechanism": "PENALIZE_FLOWFILE",
            "bulletinLevel": "WARN",
            "componentType": "PROCESSOR",
            "concurrentlySchedulableTaskCount": tasks,
            "executionNode": "ALL",
            "maxBackoffPeriod": "10 mins",
            "penaltyDuration": penalty,
            "position": self._pos(),
            "retriedRelationships": [],
            "retryCount": 10,
            "runDurationMillis": 0,
            "scheduledState": "ENABLED",
            "schedulingPeriod": period,
            "schedulingStrategy": "TIMER_DRIVEN",
            "style": {},
            "yieldDuration": "1 sec",
        }
        self.procs.append(c)
        return Ref(c["identifier"], self.id, "PROCESSOR", name)

    def _port(self, kind: str, name: str) -> Ref:
        self._unique(name)
        c = {
            "allowRemoteAccess": False,
            "componentType": kind,
            "concurrentlySchedulableTaskCount": 1,
            "groupIdentifier": self.id,
            "identifier": uid(f"{self.path}/{name}"),
            "name": name,
            "portFunction": "STANDARD",
            "position": self._pos(),
            "scheduledState": "ENABLED",
            "type": kind,
        }
        (self.inputs if kind == "INPUT_PORT" else self.outputs).append(c)
        return Ref(uid(f"{self.path}/{name}"), self.id, kind, name)

    def input(self, name: str = "in") -> Ref:
        return self._port("INPUT_PORT", name)

    def output(self, name: str = "out") -> Ref:
        return self._port("OUTPUT_PORT", name)

    def port(self, name: str) -> Ref:
        """A port of this group by name, for a connection in the parent."""
        for c in self.inputs + self.outputs:
            if c["name"] == name:
                return Ref(c["identifier"], self.id, c["type"], name)
        raise KeyError(f"{self.path}: no port {name!r}")

    def group(self, name: str, *, barrier: bool = False) -> "Group":
        self._unique(name)
        self._pos()  # reserve a grid cell
        g = Group(self.defs, name, f"{self.path}/{name}", self.context, self.id, barrier)
        self.groups.append(g)
        return g

    def connect(self, src: Ref, dst: Ref, *rels: str) -> None:
        if src.kind == "PROCESSOR" and not rels:
            raise ValueError(f"{self.path}: {src.name} -> {dst.name} names no relationship")
        self.conns.append(
            {
                "backPressureDataSizeThreshold": "1 GB",
                "backPressureObjectThreshold": 10000,
                "bends": [],
                "componentType": "CONNECTION",
                "destination": {
                    "groupId": dst.group,
                    "id": dst.id,
                    "name": dst.name,
                    "type": dst.kind,
                },
                "flowFileExpiration": "0 sec",
                "groupIdentifier": self.id,
                "identifier": uid(f"{self.path}/{src.id}/{','.join(sorted(rels))}/{dst.id}"),
                "labelIndex": 1,
                "loadBalanceCompression": "DO_NOT_COMPRESS",
                "loadBalanceStrategy": "DO_NOT_LOAD_BALANCE",
                "prioritizers": [],
                "selectedRelationships": sorted(rels),
                "source": {
                    "comments": "",
                    "groupId": src.group,
                    "id": src.id,
                    "name": src.name,
                    "type": src.kind,
                },
                "zIndex": 0,
            }
        )

    def to_json(self, position: dict[str, float] | None = None) -> dict[str, Any]:
        out: dict[str, Any] = {
            "comments": "",
            "componentType": "PROCESS_GROUP",
            "connections": self.conns,
            "controllerServices": self.services,
            "defaultBackPressureDataSizeThreshold": "1 GB",
            "defaultBackPressureObjectThreshold": 10000,
            "defaultFlowFileExpiration": "0 sec",
            "executionEngine": "INHERITED",
            "flowFileConcurrency": "SINGLE_FLOWFILE_PER_NODE" if self.barrier else "UNBOUNDED",
            "flowFileOutboundPolicy": "BATCH_OUTPUT" if self.barrier else "STREAM_WHEN_AVAILABLE",
            "funnels": [],
            "identifier": self.id,
            "inputPorts": self.inputs,
            "labels": [],
            "maxConcurrentTasks": 1,
            "name": self.name,
            "outputPorts": self.outputs,
            "parameterContextName": self.context,
            "position": position or {"x": 0.0, "y": 0.0},
            "processGroups": [
                g.to_json(
                    {"x": float(i % GRID_COLUMNS * 420), "y": 1200.0 + i // GRID_COLUMNS * 220}
                )
                for i, g in enumerate(self.groups)
            ],
            "processors": self.procs,
            "remoteProcessGroups": [],
            "scheduledState": "ENABLED",
            "statelessFlowTimeout": "1 min",
        }
        if self.parent:
            out["groupIdentifier"] = self.parent
        return out
