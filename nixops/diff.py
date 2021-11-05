from __future__ import annotations

import itertools
import json
from typing import (
    Any,
    AnyStr,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Generic,
    TypeVar,
    TYPE_CHECKING,
)
from nixops.logger import MachineLogger
from nixops.state import StateDict
import nixops.util

if TYPE_CHECKING:
    import nixops.deployment

# Note: redefined to avoid import loops
ResourceDefinitionType = TypeVar(
    "ResourceDefinitionType", bound="nixops.resources.ResourceDefinition"
)


class Handler:
    def __init__(
        self,
        keys: List[str],
        after: Optional[List] = None,
        handle: Optional[Callable] = None,
    ) -> None:
        if after is None:
            after = []
        if handle is None:
            self.handle = self._default_handle
        else:
            self.handle = handle
        self._keys = keys
        self._dependencies = after

    def _default_handle(self):
        """
        Method that should be implemented to handle the changes
        of keys returned by get_keys()
        This should be done currently by monkey-patching this method
        by passing a resource state method that realizes the change.
        """
        raise NotImplementedError

    def get_deps(self) -> List[Handler]:
        return self._dependencies

    def get_keys(self, *_: AnyStr) -> List[str]:
        return self._keys


class Diff(Generic[ResourceDefinitionType]):
    """
    Diff engine main class which implements methods for doing diffs between
    the state/config and generating a plan: sequence of handlers to be executed.
    """

    SET = 0
    UPDATE = 1
    UNSET = 2

    def __init__(
        self,
        depl: nixops.deployment.Deployment,
        logger: MachineLogger,
        defn: ResourceDefinitionType,
        state: StateDict,
        res_type: str,
    ) -> None:
        self.handlers: List[Handler] = []
        self._definition = defn.resource_eval
        self._state = state
        self._depl = depl
        self._type = res_type
        self.logger = logger
        self._diff = {}  # type: Dict[str, int]
        self._reserved = [
            "index",
            "state",
            "_type",
            "deployment",
            "_name",
            "name",
            "creationTime",
        ]

    def set_reserved_keys(self, keys: List[str]) -> None:
        """
        Reserved keys are nix options or internal state keys that we don't
        want them to trigger the diff engine so we simply ignore the diff
        of the reserved keys.
        """
        self._reserved.extend(keys)

    def get_keys(self) -> List[str]:
        diff = [k for k in self._diff if k not in self._reserved]
        return diff

    def plan(self, show: bool = False) -> List[Handler]:
        """
        This will go through the attributes of the resource and evaluate
        the diff between definition and state then return a sorted list
        of the handlers to be called to realize the diff.
        """
        keys = list(set(self._state.keys()) | set(self._definition.keys()))
        state_dict = {k: self._state.get(k) for k in keys}
        state_dict = {k: self._state.get(k) for k in keys}

        for k in keys:
            self.eval_resource_attr_diff(k)

        for k in self.get_keys():
            definition = self.get_resource_definition(k)
            if show:
                if self._diff[k] == self.SET:
                    self.logger.log(
                        "will set attribute {0} to {1}".format(k, definition)
                    )
                elif self._diff[k] == self.UPDATE:
                    self.logger.log(
                        "{0} will be updated from {1} to {2}".format(
                            k, self._state[k], definition
                        )
                    )
                else:
                    self.logger.log(
                        "will unset attribute {0} with previous value {1} ".format(
                            k, self._state[k]
                        )
                    )
        return self.get_handlers_sequence()

    def set_handlers(self, handlers: List[Handler]) -> None:
        self.handlers = handlers

    def topological_sort(self, handlers: List[Handler]) -> List[Handler]:
        """
        Implements a topological sort of a direct acyclic graph of
        handlers using the depth first search algorithm.
        The output is a sorted sequence of handlers based on their
        dependencies.
        """
        # TODO implement cycle detection
        parent: Dict[Handler, Optional[Handler]] = {}
        sequence: List[Handler] = []

        def visit(handler: Handler) -> None:
            for v in handler.get_deps():
                if v not in parent:
                    parent[v] = handler
                    visit(v)
            sequence.append(handler)

        for h in handlers:
            if h not in parent:
                parent[h] = None
                visit(h)

        return [h for h in sequence if h in handlers]

    def get_handlers_sequence(self, combinations: int = 1) -> List[Handler]:
        if len(self.get_keys()) == 0:
            return []
        h_tuple: Tuple[Handler, ...]
        for h_tuple in itertools.combinations(self.handlers, combinations):
            keys: List[str] = []
            for item in h_tuple:
                keys.extend(item.get_keys())
            if combinations == len(self.handlers):
                keys_not_found = set(self.get_keys()) - set(keys)
                if len(keys_not_found) > 0:
                    raise Exception(
                        "Couldn't find any combination of handlers"
                        " that realize the change of {0} for resource type {1}".format(
                            str(keys_not_found), self._type
                        )
                    )
            if set(self.get_keys()) <= set(keys):
                handlers_seq = self.topological_sort(list(h_tuple))
                return handlers_seq
        return self.get_handlers_sequence(combinations + 1)

    def eval_resource_attr_diff(self, key: str) -> None:
        s = self._state.get(key, None)
        d = self.get_resource_definition(key)

        if s is None and d is not None:
            self._diff[key] = self.SET
        elif s is not None and d is None:
            self._diff[key] = self.UNSET
        elif s is not None and d is not None:
            # d = json.dumps(d, cls=nixops.util.NixopsEncoder)
            # s = json.dumps(s, cls=nixops.util.NixopsEncoder)
            if s != d:
                self._diff[key] = self.UPDATE

    def get_resource_definition(self, key: str) -> Any:
        def retrieve_def(d):
            # type: (Any) -> Any
            if isinstance(d, str) and d.startswith("res-"):
                name = d[4:].split(".")[0]
                res_type = d.split(".")[1]
                k = d.split(".")[2] if len(d.split(".")) > 2 else key
                res = self._depl.get_generic_resource(name, res_type)
                if res.state != res.UP:
                    return "computed"
                try:
                    d = getattr(res, k)
                except AttributeError:
                    # TODO res._state is private and should not be reached in to.
                    # Make sure nixops-aws still works when fixing this.
                    d = res._state[k]  # type: ignore
            return d

        d = self._definition.get(key, None)
        if isinstance(d, list):
            options = []
            for option in d:
                item = retrieve_def(option)
                options.append(item)
            return options
        # if isinstance(d, dict):
        #     options ={}
        #     for option in d:
        #         item = retrieve_def(option)
        #         options.append(item)
        #     return options
        d = retrieve_def(d)
        return d
