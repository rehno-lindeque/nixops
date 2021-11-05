# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import nixops.util
from threading import Event
from nixops.monkey import Protocol, runtime_checkable
from typing import (
    List,
    Optional,
    Dict,
    Any,
    TypeVar,
    Union,
    TYPE_CHECKING,
    ResourceReferenceOption,
    Type,
    Iterable,
    Set,
    Callable
)
import typing
from nixops.state import StateDict, RecordId
from nixops.diff import Diff, Handler
from nixops.util import ImmutableMapping, ImmutableValidatedObject
from nixops.logger import MachineLogger
from typing_extensions import Literal

if TYPE_CHECKING:
    import nixops.deployment

# ResourceOptionReference could be implemented as NewType["ResourceOptionReference", T],
# but Annotated maintains backward compatibility for now
T = TypeVar
ResourceReferenceOption = Annotated[Union[T, str], "ResourceReferenceOption"]

class ResourceEval(ImmutableMapping[Any, Any]):
    pass


class ResourceOptions(ImmutableValidatedObject):
    pass


class ResourceDefinition:
    """Base class for NixOps resource definitions."""

    resource_eval: ResourceEval
    config: ResourceOptions

    @classmethod
    def get_type(cls: Type[ResourceDefinition]) -> str:
        """A resource type identifier that must match the corresponding ResourceState class"""
        raise NotImplementedError("get_type")

    @classmethod
    def get_resource_type(cls: Type[ResourceDefinition]) -> str:
        """A resource type identifier corresponding to the resources.<type> attribute in the Nix expression"""
        return cls.get_type()

    def __init__(self, name: str, config: ResourceEval):
        config_type: Union[Type, str] = self.__annotations__.get(
            "config", ResourceOptions
        )

        if isinstance(config_type, str):
            if config_type == "ResourceOptions":
                raise TypeError(
                    f'{self.__class__} is missing a "config" attribute, for example: `config: nixops.resources.ResourceOptions`, see https://nixops.readthedocs.io/en/latest/plugins/authoring.html'
                )
            else:
                raise TypeError(
                    f"{self.__class__}.config's type annotation is not allowed to be a string, see: https://nixops.readthedocs.io/en/latest/plugins/authoring.html"
                )

        if not issubclass(config_type, ResourceOptions):
            raise TypeError(
                '"config" type annotation must be a ResourceOptions subclass'
            )

        self.resource_eval = config
        self.config = config_type(**config)
        self.name = name

        if not re.match("^[a-zA-Z0-9_\-][a-zA-Z0-9_\-\.]*$", self.name):  # noqa: W605
            raise Exception("invalid resource name ‘{0}’".format(self.name))

    # TODO: return type
    def resolve_config(environment: dict):
        pass

    def get_config_references() -> Set[str]:
        def unpack_type_roots(t: Type) -> Iterator[Any]:
            """
            Unpack type arguments recursively. This function will always yield at least one type argument.

            tuple(unpack_type_roots(int)) == (int,)
            tuple(unpack_type_roots(Optional[int])) == (int, type(None))
            tuple(unpack_type_roots(Optional[Sequence[int]])) == (int, type(None))
            tuple(unpack_type_roots(Annotated[Optional[int], "x"])) == (int, type(None), "x")
            tuple(unpack_type_roots(Optional[Annotated[int, "x"]])) == (int, "x", type(None))
            """
            if typing.get_args(t) == ():
                yield t
            else:
                for arg in typing.get_args(t):
                    yield from unpack_type_roots(arg)

        def search_iterable(val, cond: Callable[..., bool]):
            """
            Recursively filter an iterable to find elements that match a given condition.

            tuple(
                unpack_iterable(
                    [(1, {"a": ("x", ExampleOptions())})],
                    (lambda x: isinstance(x, ExampleOptions) or not isinstance(x, Iterable)
                )
            ) == (1, "x", ExampleOptions())
            """
            if cond(val):
                yield val
            elif isinstance(val, Iterable):
                for val in val:
                    yield from search_iterable(val, cond)

        def collect_instances(values: Iterable, t: Type) -> Iterable:
            for val in values:
                if isinstance(val, t):
                    yield val

        def collect_resource_options(value) -> Iterable[ResourceOptions]:
            """
            Recurse iterables to find all ResourceOptions instances.
            """
            if isinstance(value, ResourceOptions):
                yield value
            if isinstance(value, Iterable):
                for val in values:
                    if isinstance(val, ResourceOptions):
                        yield val

        def is_resource_reference_option_type(t: Type) -> bool:
            return typing.get_origin(t) is Annotated and typing.get_args(t)[1:2] == ("ResourceReferenceOption",)

            # for t in types:
            #     if is_resource_reference_option_type(t):
            #         yield t
            #     else:
            #         type_args = typing.get_args(t)
            #         if type_args == ():
            #             yield t
            #         else:
            #             yield from collect_reference_types(type_args)

        # def collect_iterable_types(types: Iterable[Type]) -> Iterable[Type[Iterable]]:
        #     for t in types:
        #         if inspect.isclass(t) and issubclass(t, Iterable):
        #             yield t
        #         else:
        #             origin = typing.get_origin(t)
        #             if inspect.isclass(origin) and issubclass(origin, Iterable):
        #                 yield t
        #             else:
        #                 yield from collect_iterable_types(typing.get_args(t))

        # def reconcile_type(val, t: Type) -> Optional[Type]:
        #     """
        #     Reconcile a type with a value (and return the reconciled type)
        #     """
        #     if inspect.isclass(t):
        #         if isinstance(val, t):
        #             return (val, t)
        #     else:
        #         origin = typing.get_origin(t)
        #         if inspect.isclass(origin):
        #             if isinstance(val, origin)
        #                 return(val, t)
        #         elif origin is Union:
        #             for type_arg in typing.type_args(t):
        #                 reconciled_type_arg = reconcile_type(val, t):
        #                 if reconciled_type_arg is not None:
        #                     return reconciled_type_arg
        #             return None
        #         else:
        #             raise Exception("Unknown type origin ‘{0}’ in type ‘{1}’".format(origin, t))

        # def collect_value_types(val, types: Iterable[Type]) -> Iterable[Tuple[Any, Type]]:
        #     for t in types:
        #         if inspect.isclass(t):
        #             if isinstance(val, t):
        #                 yield (val, t)
        #                 return
        #         else:
        #             origin = typing.get_origin(t)
        #             if inspect.isclass(origin):
        #                 if isinstance(val, origin):
        #                     r = collect_value_types(val, typing.get_args(t))

        #             # if inspect.isclass(origin) and isinstance(val, origin) and issubclass(origin, Iterable):
        #             #     for v in values:
        #             #         yield from collect_value_types(v, typing.get_args(t))
        #             # else:
        #             #     yield from collect_iterable_types(typing.get_args(t))

        # def collect_noniterables(val):
        #     if isinstance(val, Iterable):
        #         for v in val:
        #             yield from collect_references(v)
        #     else:
        #         yield val

        # def collect_leafy_types(t: Type) -> Iterable[Type[ResourceReferenceOption]]:
        #     type_origin = typing.get_origin(t)
        #     type_args = typing.get_args(t)
        #     if len(type_args) == 0:
        #         yield t
        #         return

        #     if type_origin is Union:
        #         # Ignore Optional/Union types with None
        #         leafy_type_args = tuple(arg for arg in type_args if arg is not type(None))
        #     elif origin_is_class and issubclass(type_origin, Mapping):
        #         # Ignore key type argument
        #         leafy_type_args = type_args[1:]
        #     else:
        #         leafy_type_args = type_args


        #     if len(leafy_type_args) == 1:
        #         yield from collect_leafy_types(leafy_type_args[0])
        #     else:
        #         if inspect.isclass(type_origin) and issubclass(type_origin, Mapping):
        #             # Ignore key type argument
        #             for arg in type_args[1:]
        #                 yield from collect_leafy_types(

        # def collect_reference_option_types(t: Type) -> Iterable[Type[ResourceReferenceOption]]:
        #     return (leafy_type for leafy_type in collect_leafy_types(t) if is_resource_reference_option_type(t))


        def collect_references(config) -> Iterable[Tuple[T, Set[Type]]]:
            """
            Collect all reference values along with type information about the reference types.
            """
            def collect_reference_option_types(t: Type) -> Iterable[Type[ResourceReferenceOption]]:
                if is_reference_option_type(t):
                    yield t
                else:
                    for type_arg in typing.get_args(t):
                        yield from collect_reference_option_types(type_arg)
            for resource_options in collect_resource_options(config):
                for k, t in get_type_hints(resource_options, include_extras=True).items():
                    val = config.get(k)

                    if val is None:
                        continue

                    if is_resource_reference_option_type(t):
                        yield (val, t)
                        continue

                    # Handle Unions with ResourceReferenceOption
                    type_origin = typing.get_origin(t)
                    type_args = tuple(set(typing.get_args(t)) - {type(None)})
                    if type_origin is Union and len(type_args) == 1 and is_resource_reference_option_type(type_args[0]):
                        # Allow Optional[ResourceReferenceOption]
                        yield (val, type_args[0])
                    else:
                        # Disallow complex datastructures containing references
                        # TODO: Complex datastructures (arbitrary Union and Iterable types) could be supported if ReferenceOptionType can be serialized into a NewType in future.
                        if any(True for _ in collect_reference_option_types(t)):
                            raise Exception("Bug: ‘{0}.{1}’ is too complex for reference resolution.\nAvoid embedding ResourceReferenceOption in ‘{2}’.".format(type(resource_options).__name__, k, type_origin))


                    # Error if references in incompatble positions

                    # if len(leaf_types) == 1 and len(reference_types) == 1:
                    #     yield from collect_noniterables(val)
                    # else:
                    #     ...

                    # if isinstance(val, Iterable) and len(leaf_types) == 1:
                    #     if 
                    #     for collect_references()


                    # if len(leaf_types) > 1:

                    # if len(reference_type) == 0:
                    #     continue
                    # else:
                    #     for val in collect_noniterables(self.config.get(k)):
                    #         if val is not None:
                    #             yield (val, reference_types)

            # type_roots = set(unpack_type_roots(config_type))
            # options_types = set(
            #     t
            #     for t in type_roots
            #     if inspect.isclass(t) and issubclass(t, config_type)
            # )

            # if len(options_types) + len(reference_types) == 0:
            #     return
            # elif len(options_types) + len(reference_types) > 1:
            #     raise Exception(
            #         "Bug: ResourceOptions type is too complex for reference resolution.\nAvoid Unions with multiple ResourceOptions or ResourceReferences."
            #     )

            # if reference_types == {"ResourceReferenceOption"}:
            #     for val in search_iterable(config_value, lambda x: isinstance(x, str)):
            #         yield val
            # else:
            #     (options_type,) = tuple(options_types)

            # if typing.get_origin(config_type) is not None:
            #     # Collect references from fully typed Iterables containing (no more than one) ResourceOption
            #     # (e.g. List[...], Tuple[...], Dict[...], Set[...])

            #     if isinstance(config_value, Iterable) and len(type_roots) == 1:
            #         (options_type,) = tuple(options_types)
            #         for val in config_value:
            #             yield from collect_references(
            #                 config_value=val,
            #                 config_type=options_type
            #                 if isinstance(val, ResourceOptions)
            #                 else config_type,
            #             )
            #     return

            # # Collect references from dicts
            # type_hints = get_type_hints(config_type, include_extras=True)
            # if type_hints == {}:
            #     return
            #     if not isinstance(config_value, config_type):
            #         raise Exception("Bug: config_value and config_type does not match")

            #         for val in config_value:
            #             yield from collect_references(
            #                 config_value=val,
            #                 config_type=options_type
            #                 if isinstance(val, ResourceOptions)
            #                 else config_type,
            #             )
            #         yield from collect_refe


            # for k, t in get_type_hints(config_type, include_extras=True).items():
            #     for c in unpack_type_roots(t):
            #         if isinstance(c, ResourceOptions):
            #             yield from collect_references(config["k"])
            #             break
            #         elif typing.get_origin(t) is Annnotated and typing.get_args(t)[1:2] == (
            #             "ResourceOptionReference",
            #         ):
            #             ref = self.config.get("k")
            #             if isinstance(ref, str) and ref.startswith("res-"):
            #                 yield ref

        print("!!!!!!!", set(collect_references(self.config)))

        return set(collect_references(self.config))

    def show_type(self) -> str:
        """A short description of the type of resource this is"""
        return self.get_type()


ResourceDefinitionType = TypeVar("ResourceDefinitionType", bound="ResourceDefinition")


@runtime_checkable
class ResourceState(Protocol[ResourceDefinitionType]):
    """Base class for NixOps resource state objects."""

    definition_type: Type[ResourceDefinitionType]

    name: str

    @classmethod
    def get_type(cls) -> str:
        """A resource type identifier that must match the corresponding ResourceDefinition class"""
        raise NotImplementedError("get_type")

    # Valid values for self.state.  Not all of these make sense for
    # all resource types.
    UNKNOWN: Literal[0] = 0  # state unknown
    MISSING: Literal[1] = 1  # instance destroyed or not yet created
    STARTING: Literal[2] = 2  # boot initiated
    UP: Literal[3] = 3  # machine is reachable
    STOPPING: Literal[4] = 4  # shutdown initiated
    STOPPED: Literal[5] = 5  # machine is down
    UNREACHABLE: Literal[6] = 6  # machine should be up, but is unreachable
    RESCUE: Literal[7] = 7  # rescue system is active for the machine

    state: Union[
        Literal[0],
        Literal[1],
        Literal[2],
        Literal[3],
        Literal[4],
        Literal[5],
        Literal[6],
        Literal[7],
    ] = nixops.util.attr_property("state", UNKNOWN, int)
    index: Optional[int] = nixops.util.attr_property("index", None, int)
    obsolete: bool = nixops.util.attr_property("obsolete", False, bool)

    # Time (in Unix epoch) the resource was created.
    creation_time: Optional[int] = nixops.util.attr_property("creationTime", None, int)

    _created_event: Optional[Event] = None
    _destroyed_event: Optional[Event] = None
    _errored: Optional[bool] = None

    # While this looks like a rookie mistake where the list is going  get shared
    # across all class instances it's not... It's working around a Mypy crash.
    #
    # We're overriding this value in __init__.
    # It's safe despite there being a shared list on the class level
    _wait_for: List["ResourceState"] = []

    depl: nixops.deployment.Deployment
    id: RecordId
    logger: MachineLogger

    def __init__(self, depl: nixops.deployment.Deployment, name: str, id: RecordId):
        # Override default class-level list.
        # Previously this behaviour was missing and the _wait_for list was shared across all instances
        # of ResourceState, resulting in a deadlock in resource destruction as they resource being
        # destroyed had a reference to itself in the _wait_for list.
        self._wait_for = []
        self.depl = depl
        self.name = name
        self.id = id
        self.logger = depl.logger.get_logger_for(name)
        if self.index is not None:
            self.logger.register_index(self.index)

    def _set_attrs(self, attrs: Dict[str, Any]) -> None:
        """Update machine attributes in the state file."""
        with self.depl._db:
            c = self.depl._db.cursor()
            for n, v in attrs.items():
                if v is None:
                    c.execute(
                        "delete from ResourceAttrs where machine = ? and name = ?",
                        (self.id, n),
                    )
                else:
                    c.execute(
                        "insert or replace into ResourceAttrs(machine, name, value) values (?, ?, ?)",
                        (self.id, n, v),
                    )

    def _set_attr(self, name: str, value: Any) -> None:
        """Update one machine attribute in the state file."""
        self._set_attrs({name: value})

    def _del_attr(self, name: str) -> None:
        """Delete a machine attribute from the state file."""
        with self.depl._db:
            self.depl._db.execute(
                "delete from ResourceAttrs where machine = ? and name = ?",
                (self.id, name),
            )

    def _get_attr(self, name: str, default=nixops.util.undefined) -> Any:
        """Get a machine attribute from the state file."""
        with self.depl._db:
            c = self.depl._db.cursor()
            c.execute(
                "select value from ResourceAttrs where machine = ? and name = ?",
                (self.id, name),
            )
            row = c.fetchone()
            if row is not None:
                return row[0]
            return nixops.util.undefined

    def export(self) -> Dict[str, Dict[str, str]]:
        """Export the resource to move between databases"""
        with self.depl._db:
            c = self.depl._db.cursor()
            c.execute(
                "select name, value from ResourceAttrs where machine = ?", (self.id,)
            )
            rows = c.fetchall()
            res = {row[0]: row[1] for row in rows}
            res["type"] = self.get_type()
            return res

    def import_(self, attrs: Dict):
        """Import the resource from another database"""
        with self.depl._db:
            for k, v in attrs.items():
                if k == "type":
                    continue
                self._set_attr(k, v)

    # XXX: Deprecated, use self.logger.* instead!
    def log(self, *args, **kwargs) -> None:
        return self.logger.log(*args, **kwargs)

    def log_end(self, *args, **kwargs) -> None:
        return self.logger.log_end(*args, **kwargs)

    def log_start(self, *args, **kwargs) -> None:
        return self.logger.log_start(*args, **kwargs)

    def log_continue(self, *args, **kwargs) -> None:
        return self.logger.log_continue(*args, **kwargs)

    def warn(self, *args, **kwargs) -> None:
        return self.logger.warn(*args, **kwargs)

    def success(self, *args, **kwargs) -> None:
        return self.logger.success(*args, **kwargs)

    # XXX: End deprecated methods

    def show_type(self) -> str:
        """A short description of the type of resource this is"""
        return self.get_type()

    def show_state(self) -> str:
        """A description of the resource's current state"""
        state = self.state
        if state == self.UNKNOWN:
            return "Unknown"
        elif state == self.MISSING:
            return "Missing"
        elif state == self.STARTING:
            return "Starting"
        elif state == self.UP:
            return "Up"
        elif state == self.STOPPING:
            return "Stopping"
        elif state == self.STOPPED:
            return "Stopped"
        elif state == self.UNREACHABLE:
            return "Unreachable"
        elif state == self.RESCUE:
            return "Rescue"
        else:
            raise Exception("machine is in unknown state")

    def prefix_definition(self, attr):
        """Prefix the resource set with a py2nixable attrpath"""
        raise Exception("not implemented")

    def get_physical_spec(self):
        """py2nixable physical specification of the resource to be fed back into the network"""
        return {}

    def get_physical_backup_spec(self, backupid):
        """py2nixable physical specification of the specified backup"""
        return []

    @property
    def resource_id(self):
        """A unique ID to display for this resource"""
        return None

    @property
    def public_ipv4(self) -> Optional[str]:
        return None

    def create_after(
        self,
        resources: Iterable[GenericResourceState],
        defn: Optional[ResourceDefinition],
    ) -> Set[GenericResourceState]:
        """Return a set of resources that should be created before this one."""
        return set()

    def destroy_before(
        self, resources: Iterable[GenericResourceState]
    ) -> Set[GenericResourceState]:
        """Return a set of resources that should be destroyed after this one."""
        return self.create_after(resources, None)

    def create(
        self,
        defn: ResourceDefinitionType,
        check: bool,
        allow_reboot: bool,
        allow_recreate: bool,
    ) -> None:
        """Create or update the resource defined by ‘defn’."""
        raise NotImplementedError("create")

    def check(
        self,
    ):  # TODO this return type is inconsistent with child class MachineState
        """
        Reconcile the state file with the real world infrastructure state.
        This should not do any provisionning but just sync the state.
        """
        self._check()

    def _check(self):
        return True

    def after_activation(self, defn: ResourceDefinition) -> None:
        """Actions to be performed after the network is activated"""
        return

    def destroy(self, wipe: bool = False) -> bool:
        """Destroy this resource, if possible."""
        self.logger.warn("don't know how to destroy resource ‘{0}’".format(self.name))
        return False

    def delete_resources(self) -> bool:
        """delete this resource state, if possible."""
        if not self.depl.logger.confirm(
            "are you sure you want to clear the state of {}? "
            "this will only remove the resource from the local "
            "NixOps state and the resource may still exist outside "
            "of the NixOps database.".format(self.name)
        ):
            return False

        self.logger.warn(
            "removing resource {} from the local NixOps database ...".format(self.name)
        )
        return True

    def next_charge_time(self) -> Optional[int]:
        """Return the time (in Unix epoch) when this resource will next incur
        a financial charge (or None if unknown)."""
        return None


@runtime_checkable
class DiffEngineResourceState(
    ResourceState[ResourceDefinitionType], Protocol[ResourceDefinitionType]
):
    _reserved_keys: List[str] = []
    _state: StateDict

    def __init__(self, depl: "nixops.deployment.Deployment", name: str, id: RecordId):
        nixops.resources.ResourceState.__init__(self, depl, name, id)
        self._state = StateDict(depl, id)

    def create(
        self,
        defn: ResourceDefinitionType,
        check: bool,
        allow_reboot: bool,
        allow_recreate: bool,
    ) -> None:
        # if --check is true check against the api and update the state
        # before firing up the diff engine in order to get the needed
        # handlers calls
        if check:
            self._check()
        diff_engine = self.setup_diff_engine(defn)

        for handler in diff_engine.plan():
            handler.handle(allow_recreate)

    def plan(self, defn: ResourceDefinitionType) -> None:
        if hasattr(self, "_state"):
            diff_engine = self.setup_diff_engine(defn)
            diff_engine.plan(show=True)
        else:
            self.logger.warn(
                "resource type {} doesn't implement a plan operation".format(
                    self.get_type()
                )
            )

    def setup_diff_engine(self, defn: ResourceDefinitionType):
        diff_engine = Diff(
            depl=self.depl,
            logger=self.logger,
            defn=defn,
            state=self._state,
            res_type=self.get_type(),
        )
        diff_engine.set_reserved_keys(self._reserved_keys)
        diff_engine.set_handlers(self.get_handlers())
        return diff_engine

    def get_handlers(self):
        return [
            getattr(self, h) for h in dir(self) if isinstance(getattr(self, h), Handler)
        ]

    def get_defn(self) -> ResourceDefinitionType:
        return self.depl.get_typed_definition(
            self.name, self.get_type(), self.definition_type
        )


GenericResourceState = ResourceState[ResourceDefinition]
