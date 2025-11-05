from typing import Mapping, Literal, TypeVar

ModelT   = TypeVar("ModelT")
InsertT  = TypeVar("InsertT")
WhereT   = TypeVar("WhereT",  bound=Mapping[str, object])
IncludeT = TypeVar("IncludeT", bound=Mapping[str, bool])
OrderByT = TypeVar("OrderByT", bound=Mapping[str, Literal['asc','desc']])
