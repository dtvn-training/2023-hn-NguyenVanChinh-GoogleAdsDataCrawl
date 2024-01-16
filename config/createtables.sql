drop table if exists DataType, SelectableWith, ResourceFieldConnect, RelatedResource, Resources, ResourceField;

create table if not exists ResourceField (
FieldId int primary key not null,
FieldName TEXT,
FieldDescription TEXT,
Category varchar(20),
TypeURL TEXT,
Filterable bool,
Selectable bool,
Sortable bool,
Repeated bool,
GoogleadsApiVersion int);

create table if not exists DataType (
Id int primary key,
FieldId int,
DataType varchar(20),
EnumDataType varchar(100),
GoogleadsApiVersion int,
foreign key (FieldId) references ResourceField(FieldId));

create table if not exists SelectableWith (
Id int primary key,
FieldId int,
ResourceName text,
GoogleadsApiVersion int,
foreign key (FieldId) references ResourceField(FieldId));

create table Resources (
ResourceId int primary key not null,
ResourceName text,
ResourceDescription text,
ResourceWithMetric bool,
GoogleadsApiVersion int
);

create table if not exists RelatedResource (
Id int primary key,
MasterResourceId int not null,
AttributedResourceId int not null,
BeSegment bool not null,
GoogleadsApiVersion int,
foreign key (MasterResourceId) references Resources(ResourceId),
foreign key (AttributedResourceId) references Resources(ResourceId));

create table if not exists ResourceFieldConnect (
Id int primary key not null,
FieldId int not null,
ResourceId int not null,
GoogleadsApiVersion int,
foreign key (FieldId) references ResourceField(FieldId),
foreign key (ResourceId) references Resources(ResourceId))