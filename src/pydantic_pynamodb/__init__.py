"""Pydantic Pynamodb."""
# set by poetry-dynamic-versioning
__version__ = "0.0.8"  # noqa: E402

from collections import namedtuple
from typing import AbstractSet
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union
from typing import cast

from pydantic import BaseModel as PydanticSchema
from pynamodb.exceptions import GetError
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.update import Action
from pynamodb.models import Model as PydnamoModel
from pynamodb.settings import OperationSettings


_T = TypeVar("_T", bound="PydanticPynamoDB")
_TM = TypeVar("_TM", bound="PydanticPynamoDB.Model")
_KeyType = Any

DynamoKeyVal = namedtuple("DynamoKeyVal", ("key", "val"))

__all__ = ["PydanticPynamoDB"]


class PydanticPynamoDB(PydanticSchema):
    """This is a pydantic schema wrapped around a PynamoDB model.

    The idea is that you interact with the pydantic object and use the
    class methods on the PydanticPydnamo to do database operations.

    Special "config variables" as part of the object:
     * _hash_key: Str or Callable. This is the key of the object to use as the dynamo hash key
     * _range_key: Optional Str or Callable. This is the key of the object to use as the dynamo range key.
    Callable Signature callable(PydanticPynamoDB) -> str
    * _key_remap : Dict[str, Str or Callable] This is a dictionary of keys to remap to the dynamo object.
    If you pass a string, the remap is a direct rename
    (i.e. name: thingName would map the value name to thingName in the dynamo object)
    Callable Signature callable(str, Any, PydanticPynamoDB) -> Tuple[str, Any]
        Take in key name, value, and the PydanticPynamoDB object. Return the new key name, and the new value.
    * _ create_if_not_exist: bool - Default false. If true, will create the object in dynamodb if it doesn't already exist
    * _auto_sync: bool - Default false. If true, will update dynamodb each time one of the pydantic attributes is updated.
    """

    _dynamo_obj: Optional[PydnamoModel] = None
    _hash_key: Union[str, Callable[[str, str, _T], Tuple[str, Any]]]
    _range_key: Optional[Union[str, Callable[[str, str, _T], Tuple[str, Any]]]]
    _key_remap: Dict[str, Union[str, Callable[[str, str, _T], Tuple[str, Any]]]] = {}
    _computed_keys: Dict[str, Callable[[str, str, _T], Tuple[str, Any]]] = {}
    _auto_sync: bool = False

    class Model(PydnamoModel):
        pass

    class Config:
        allow_population_by_field_name = True
        allow_population_by_alias = True
        orm_mode = True
        underscore_attrs_are_private = True
        extra = "ignore"

    @property
    def dynamo_obj(self) -> PydnamoModel:
        if self._dynamo_obj is None:
            try:
                if self.range_key is not None:
                    self._dynamo_obj = self.Model.get(
                        self.hash_key.val, self.range_key.val
                    )
                else:
                    self._dynamo_obj = self.Model.get(self.hash_key.val)
            except Exception as exc:
                if isinstance(exc, PydnamoModel.DoesNotExist) or isinstance(
                    exc, GetError
                ):
                    self._dynamo_obj = self.Model(**self.dict(remap_to_dynamo=True))
                    if self._auto_sync:
                        self._dynamo_obj.save()
                else:
                    raise exc
        cast(self._dynamo_obj, self.Model)
        return self._dynamo_obj

    @property
    def hash_key(self) -> DynamoKeyVal:
        """This object's hash key from dynamodb.

        Returns:
            str -- this object's hash key from dynamodb.
        """
        if isinstance(self._hash_key, Callable):
            key, val = self._hash_key("hash_key", "", self)
        else:
            key = self._hash_key
            val = getattr(self, self._hash_key)
        return DynamoKeyVal(key, val)

    @property
    def range_key(self) -> Optional[DynamoKeyVal]:
        """This object's range key, if any, from dynamodb.

        Returns:
            Optional[str] -- this object's range key, if any, from dynamodb
        """
        if self._range_key is None:
            return None

        if isinstance(self._range_key, Callable):
            key, val = self._range_key(value="", database=self)
        else:
            key = self._range_key
            val = getattr(self, self._range_key)
        return DynamoKeyVal(key, val)

    def __repr__(self) -> str:
        if self._range_key is not None:
            msg = f"{self.Model.Meta.table_name}<{self.hash_key.val}, {self.range_key.val}>"
        else:
            msg = f"{self.Model.Meta.table_name}<{self.hash_key.val}>"

        return msg

    def dict(
        self,
        *,
        include: Union[
            AbstractSet[Union[int, str]], Mapping[Union[int, str], Any]
        ] = None,
        exclude: Union[
            AbstractSet[Union[int, str]], Mapping[Union[int, str], Any]
        ] = None,
        by_alias: bool = False,
        skip_defaults: bool = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        remap_to_dynamo: bool = False,
    ) -> Dict[str, Any]:
        """
        Generate a dictionary representation of the model, optionally specifying which fields to include or exclude.

        """
        this_dict = super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )
        keys = this_dict.keys()
        if remap_to_dynamo:
            for key, transform in self._key_remap.items():
                if key in keys:
                    if isinstance(transform, Callable):
                        this_dict[key] = transform(key, this_dict[key], self)
                    else:
                        this_dict[transform] = this_dict[key]
                        del this_dict[key]
            for key, compute_function in self._computed_keys.items():
                computed_key, computed_value = compute_function(
                    key, this_dict[key], self
                )
                this_dict[computed_key] = computed_value
        return this_dict

    def __setattr__(self, name: str, value: Any):
        super().__setattr__(name, value)
        original_name = name
        original_value = value
        if name in (
            "_dynamo_obj",
            "_hash_key",
            "_range_key",
            "_key_remap",
            "_auto_sync",
        ):
            return
        if name in self._key_remap.keys():
            if isinstance(self._key_remap[name], Callable):
                name, value = self._key_remap[name](name, value, self)
            else:
                name = self._key_remap[name]
        setattr(self.dynamo_obj, name, value)
        if original_name in self._computed_keys.keys():
            computed_key, computed_value = self._computed_keys[original_name](
                original_name, original_value, self
            )
            setattr(self.dynamo_obj, computed_key, computed_value)

        if self._auto_sync:
            self.dynamo_obj.save()

    @classmethod
    def from_dynamo(cls, dynamo_obj: _TM) -> Type[_T]:
        """Creates a PydanticPynamoDB object from a pydnamodb object.

        Uses pydantic from_orm mode.

        Arguments:
            cls {Type[} -- [description]
            dynamo_obj {_TM} -- [description]

        Returns:
            Type[_T] -- [description]
        """
        pydantic_obj = cls.from_orm(dynamo_obj)
        pydantic_obj._dynamo_obj = dynamo_obj
        return pydantic_obj

    @classmethod
    def get(
        cls,
        hash_key: _KeyType,
        range_key: Optional[_KeyType] = None,
        consistent_read: bool = False,
        attributes_to_get: Optional[Sequence[str]] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> _T:
        """Returns a single object using the provided keys.


        Arguments:
            hash_key {_KeyType} -- The hash key of the desired item

        Keyword Arguments:
            range_key {Optional[_KeyType]} -- The range key of the desired item, only used when appropriate (default: {None})
            consistent_read {bool} -- [description] (default: {False})
            attributes_to_get {Optional[Sequence[Text]]} -- [description] (default: {None})
            settings {OperationSettings} -- [description] (default: {OperationSettings.default})

        Raises:
            PydanticPydnamo.Model.DoesNotExist: if the object to does not exist

        Returns:
            _T -- A PydanticPydnamo object
        """
        dynamo_obj = cls.Model.get(
            hash_key, range_key, consistent_read, attributes_to_get, settings
        )
        pydantic_obj = cls.from_dynamo(dynamo_obj)
        pydantic_obj._dynamo_obj = dynamo_obj
        return pydantic_obj

    @classmethod
    def query(
        cls: Type[_T],
        hash_key: _KeyType,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
        index_name: Optional[str] = None,
        scan_index_forward: Optional[bool] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
        attributes_to_get: Optional[Iterable[str]] = None,
        page_size: Optional[int] = None,
        rate_limit: Optional[float] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> List[Optional[_T]]:
        """
        Provides a high level query API.

        :param hash_key: The hash key to query
        :param range_key_condition: Condition for range key
        :param filter_condition: Condition used to restrict the query results
        :param consistent_read: If True, a consistent read is performed
        :param index_name: If set, then this index is used
        :param limit: Used to limit the number of results returned
        :param scan_index_forward: If set, then used to specify the same parameter to the DynamoDB API.
            Controls descending or ascending results
        :param last_evaluated_key: If set, provides the starting point for query.
        :param attributes_to_get: If set, only returns these elements
        :param page_size: Page size of the query to DynamoDB
        :param rate_limit: If set then consumed capacity will be limited to this amount per second
        """
        return [
            cls.from_dynamo(dynamodb)
            for dynamodb in cls.Model.query(
                hash_key,
                range_key_condition,
                filter_condition,
                consistent_read,
                index_name,
                scan_index_forward,
                limit,
                last_evaluated_key,
                attributes_to_get,
                page_size,
                rate_limit,
                settings,
            )
        ]

    @classmethod
    def scan(
        cls: Type[_T],
        filter_condition: Optional[Condition] = None,
        segment: Optional[int] = None,
        total_segments: Optional[int] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
        page_size: Optional[int] = None,
        consistent_read: Optional[bool] = None,
        index_name: Optional[str] = None,
        rate_limit: Optional[float] = None,
        attributes_to_get: Optional[Sequence[str]] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> List[Optional[_T]]:
        """
        Iterates through all items in the table
        :param filter_condition: Condition used to restrict the scan results
        :param segment: If set, then scans the segment
        :param total_segments: If set, then specifies total segments
        :param limit: Used to limit the number of results returned
        :param last_evaluated_key: If set, provides the starting point for scan.
        :param page_size: Page size of the scan to DynamoDB
        :param consistent_read: If True, a consistent read is performed
        :param index_name: If set, then this index is used
        :param rate_limit: If set then consumed capacity will be limited to this amount per second
        :param attributes_to_get: If set, specifies the properties to include in the projection expression
        """
        return [
            cls.from_dynamo(dynamodb)
            for dynamodb in cls.Model.scan(
                filter_condition,
                segment,
                total_segments,
                limit,
                last_evaluated_key,
                page_size,
                consistent_read,
                index_name,
                rate_limit,
                attributes_to_get,
                settings,
            )
        ]

    @classmethod
    def update_ttl(cls, ignore_update_ttl_errors: bool) -> None:
        """
        Attempt to update the TTL on the table.
        Certain implementations (eg: dynalite) do not support updating TTLs and will fail.
        """
        cls.Model.update_ttl(ignore_update_ttl_errors)

    @classmethod
    def count(
        cls: Type[_T],
        hash_key: Optional[_KeyType] = None,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
        index_name: Optional[str] = None,
        limit: Optional[int] = None,
        rate_limit: Optional[float] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> int:
        """
        Provides a filtered count
        :param hash_key: The hash key to query. Can be None.
        :param range_key_condition: Condition for range key
        :param filter_condition: Condition used to restrict the query results
        :param consistent_read: If True, a consistent read is performed
        :param index_name: If set, then this index is used
        :param rate_limit: If set then consumed capacity will be limited to this amount per second
        """
        return cls.Model.count(
            hash_key,
            range_key_condition,
            filter_condition,
            consistent_read,
            index_name,
            limit,
            rate_limit,
            settings,
        )

    @classmethod
    def exists(cls: Type[_T]) -> bool:
        """
        Returns True if this table exists, False otherwise
        """
        return cls.Model.exists()

    def save(
        self,
        condition: Optional[Condition] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> Dict[str, Any]:
        """
        Save this object to dynamodb.
        """
        return self.dynamo_obj.save(condition, settings)

    def refresh(
        self,
        consistent_read: bool = False,
        settings: OperationSettings = OperationSettings.default,
    ) -> None:
        """
        Retrieves this object's data from dynamodb and syncs this local object.

        :param consistent_read: If True, then a consistent read is performed.
        :param settings: per-operation settings
        :raises ModelInstance.DoesNotExist: if the object to be updated does not exist
        """
        self.dynamo_obj.refresh(consistent_read, settings)
        # use from_orm to run any remapping from dynamo -> pydantic
        pydantic_obj = self.__class__.from_dynamo(self.dynamo_obj)
        # Update our pydantic object with values from the new pydantic object
        self.__dict__.update(pydantic_obj.dict())
        del pydantic_obj

    def delete(
        self,
        condition: Optional[Condition] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> Any:
        """
        Deletes this object from dynamodb
        :raises pynamodb.exceptions.DeleteError: If the record can not be deleted
        """
        return self.dynamo_obj.delete(condition, settings)

    def update(
        self,
        actions: List[Action],
        condition: Optional[Condition] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> Any:
        """
        Updates an item using the UpdateItem operation.
        :param actions: a list of Action updates to apply
        :param condition: an optional Condition on which to update
        :param settings: per-operation settings
        :raises ModelInstance.DoesNotExist: if the object to be updated does not exist
        :raises pynamodb.exceptions.UpdateError: if the `condition` is not met
        """
        return self.dynamo_obj.update(actions, condition, settings)
