drop table if exists ResourceFieldTmp, DataTypeTmp, SelectableWithTmp, ResourceFieldConnectTmp, RelatedResourceTmp, ResourceTmp;

create table ResourceFieldTmp (
FieldId int primary key not null,
FieldName TEXT,
FieldDescription TEXT,
Category varchar(20),
TypeURL TEXT,
Filterable bool,
Selectable bool,
Sortable bool,
Repeated bool);

create table DataTypeTmp (
Id int primary key,
FieldId int,
DataType varchar(20),
EnumDataType varchar(100),
foreign key (FieldId) references ResourceFieldTmp(FieldId));

create table SelectableWithTmp (
Id int primary key,
FieldId int,
ResourceName text,
foreign key (FieldId) references ResourceFieldTmp(FieldId));

create table ResourceTmp (
ResourceId int primary key not null,
ResourceName text,
ResourceDescription text,
ResourceWithMetric bool
);

create table RelatedResourceTmp (
Id int primary key,
MasterResourceId int not null,
AttributedResourceId int not null,
BeSegment bool not null,
foreign key (MasterResourceId) references ResourceTmp(ResourceId),
foreign key (AttributedResourceId) references ResourceTmp(ResourceId));

create table ResourceFieldConnectTmp (
Id int primary key not null,
FieldId int not null,
ResourceId int not null,
foreign key (FieldId) references ResourceFieldTmp(FieldId),
foreign key (ResourceId) references ResourceTmp(ResourceId))