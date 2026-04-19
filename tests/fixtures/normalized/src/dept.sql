-- source has lowercase + storage clauses + reordered constraints
create table hr.dept (
  id number(5) not null,
  name varchar2(50),
  constraint sys_c009988 check (id > 0),
  constraint pk_dept primary key (id)
) tablespace users pctfree 10 storage (initial 64k);
