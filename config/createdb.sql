drop table if exists relatedresource, resource, resourcefield, datatype, selectablewith, resourcefieldconnect;

create table if not exists ResourceField (
FieldId int primary key not null,
FieldName TEXT,
FieldDescription TEXT,
Category varchar(20),
TypeURL TEXT,
Filterable bool,
Selectable bool,
Sortable bool,
Repeated bool);

create table if not exists DataType (
Id int primary key,
FieldId int,
DataType varchar(100),
foreign key (FieldId) references resourcefield(FieldId));

create table if not exists SelectableWith (
Id int primary key,
FieldId int,
ResourceName text,
foreign key (FieldId) references resourcefield(FieldId));

create table Resource (
ResourceId int primary key not null,
ResourceName text,
ResourceDescription text,
ResourceWithMetric bool
);

create table if not exists RelatedResource (
Id int primary key,
MasterResourceId int not null,
AttributedResourceId int not null,
beSegment bool not null,
foreign key (MasterResourceId) references Resource(ResourceId),
foreign key (AttributedResourceId) references Resource(ResourceId));

create table if not exists ResourceFieldConnect (
Id int primary key not null,
FieldId int not null,
ResourceId int not null,
foreign key (FieldId) references ResourceField(FieldId),
foreign key (ResourceId) references Resource(ResourceId))