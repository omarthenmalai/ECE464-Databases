create table employees(
	eid int PRIMARY KEY,
	ename char(20),
	hourly_wage int
);

create table shifts(
	eid int,
	shift_start datetime,
	shift_end datetime,
	PRIMARY KEY(eid, shift_start, shift_end)
);

insert into employees values (5, "rishi", 15);
insert into employees values (2, "ryan", 18);
insert into employees values (3, "harris", 19);
insert into employees values (4, "omar", 5);

insert into shifts values (5, '1998/10/10 10:00:00', '1998/10/10 16:00:00');
insert into shifts values (2, '1998/10/10 8:00:00', '1998/10/10 14:00:00');
insert into shifts values (3, '1998/10/10 9:00:00', '1998/10/10 17:00:00');
insert into shifts values (4, '1998/10/10 06:00:00', '1998/10/10 12:00:00');
