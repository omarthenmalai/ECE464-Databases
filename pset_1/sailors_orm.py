from sqlalchemy import create_engine, Integer, Float, String, Column, DateTime, ForeignKey, PrimaryKeyConstraint, func, text, desc, distinct
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, backref, relationship, aliased
import pytest

engine = create_engine('mysql+pymysql://root:123456@127.0.0.1:3306/pset_1')

conn = engine.connect()

session = sessionmaker(bind=engine)
s = session()

Base = declarative_base()

date = "1998/10/10"
	
class Sailor(Base):
    __tablename__ = "sailors"
    
    sid = Column(Integer, primary_key=True)
    sname = Column(String)
    rating = Column(Integer)
    age = Column(Integer)

    def __repr__(self):
        return "<Sailor(sid={0}, sname={1}, rating={2}, age={3})>".format(self.sid, self.sname, self.rating, self.age)
class Boat(Base):
	__tablename__ = "boats"
    
	bid = Column(Integer, primary_key=True)
	bname = Column(String)
	color = Column(String)
	daily_price = Column(Integer)
	daily_cost = Column(Integer)
	
	reservations = relationship("Reservation", backref=backref('boat', cascade='delete'))
    
	def __repr__(self):
		return "<Boat(bid={0}, bname={1}, color={2})>".format(self.bid, self.bname, self.color)

class Reservation(Base):
    __tablename__ = "reserves"
    __table_args__ = (PrimaryKeyConstraint('sid', 'bid', 'day'), {})
    
    sid = Column(Integer, ForeignKey("sailors.sid"))
    bid = Column(Integer, ForeignKey("boats.bid"))
    day = Column(DateTime)
    
    sailor = relationship("Sailor")
    
    def __repr__(self):
        return "<Reservation(sid={0}, bid={1}, day={2})>".format(self.sid, self.bid, self.day)

		
class Employee(Base):
	__tablename__ = "employees"
	
	eid = Column(Integer, primary_key=True)
	ename = Column(String)
	hourly_wage = Column(Integer)

	reservations = relationship("Shift", backref=backref('employees', cascade='delete'))

	
	def __repr__(self):
		return "<Employee(eid={0}, ename={1}, hourly_wage={2})>".format(self.eid, self.ename, self.hourly_wage)

class Shift(Base):
	__tablename__ = "shifts"
	__table_args__ = (PrimaryKeyConstraint('eid', 'shift_start', 'shift_end'), {})
	
	eid = Column(Integer, ForeignKey("employees.eid"))
	shift_start = Column(DateTime)
	shift_end = Column(DateTime)

	def __repr__(self):
		return "<Shift(eid={0}, shift_start={1}, shift_end={2})>".format(self.eid, self.shift_start, self.shift_end)
	
def query_check(orm_query, strsql):
	'''
		runs the two queries, using the ORM and using the SQL string, and compares the outputs.
	'''
	orm_query_records = []
	sql_query_records = []
	
	sql_query = conn.execute(strsql)

	for result in orm_query:
		orm_query_records.append(result)
	
	for result in sql_query:
		sql_query_records.append(result)
		
	return sql_query_records == orm_query_records
	
def test_1():
	strsql = (
		"SELECT boats.bid, boats.bname, COUNT(*) as num_reservations "
		"FROM boats, reserves "
		"WHERE boats.bid = reserves.bid "
		"GROUP BY boats.bid "
		"HAVING num_reservations > 0;"
	)
	orm_query = s.query(Boat.bid, Boat.bname, func.count("*")).filter(Boat.bid == Reservation.bid).group_by(Boat.bid).having(func.count("*") > 0)
	assert query_check(orm_query, strsql)
	
	
def test_2():
	strsql = (
		"SELECT sailors.sid, sailors.sname "
		"FROM sailors , ( " 
			"SELECT reserves.sid,COUNT(DISTINCT(reserves.bid)) AS count_of_red "
			"FROM reserves, boats, sailors "
			"WHERE reserves.bid = boats.bid "
			"AND reserves.sid = sailors.sid "
			"AND boats.color = \"red\" "
			"GROUP BY reserves.sid "
		") AS temp "
		"WHERE sailors.sid = temp.sid "
		"AND temp.count_of_red = (SELECT COUNT(*) "
									"FROM boats "
									"WHERE boats.color = \"red\");"
	)
		
	subquery = s.query(Reservation.sid, func.count(distinct(Reservation.bid)).label("count_of_red"))\
				.filter(Reservation.bid == Boat.bid)\
				.filter(Reservation.sid == Sailor.sid)\
				.filter(Boat.color == 'red')\
				.group_by(Reservation.sid)\
				.subquery()
	subquery_2 = s.query(func.count("*")).filter(Boat.color == 'red')
	orm_query = s.query(Sailor.sid, Sailor.sname)\
				.filter(Sailor.sid == subquery.c.sid)\
				.filter(subquery.c.count_of_red == subquery_2)

	
	
	
	assert query_check(orm_query, strsql)

def test_3():
	strsql = (
		"SELECT DISTINCT sailors.sid, sailors.sname "
		"FROM sailors, boats, reserves "
		"WHERE sailors.sid = reserves.sid "
		"AND reserves.bid = boats.bid "
		"AND boats.color = \"red\" "
		"AND sailors.sid NOT IN "
		"( SELECT sailors.sid "
		"FROM sailors, boats, reserves "
		"WHERE sailors.sid = reserves.sid "
		"AND boats.bid = reserves.bid "
		"AND boats.color != \"red\" );"
	)
	sailorAlias = aliased(Sailor)
	subquery = s.query(Sailor.sid)\
					.join(Reservation)\
					.join(Boat)\
					.filter(Boat.color != "red")\
					.subquery()
	orm_query = s.query(Sailor.sid, Sailor.sname)\
					.distinct()\
					.filter(Reservation.sid == Sailor.sid)\
					.filter(Boat.bid == Reservation.bid)\
					.filter(Boat.color == "red")\
					.filter(Sailor.sid.notin_(subquery))
	assert query_check(orm_query, strsql)
	
	
def test_4():
	strsql = (
		"SELECT boats.bid, boats.bname, COUNT(*) as num_reservations "
		"FROM boats, reserves "
		"WHERE boats.bid = reserves.bid "
		"GROUP BY boats.bid "
		"ORDER BY num_reservations DESC "
		"LIMIT 1;"
	)
	
	orm_query = s.query(Boat.bid, Boat.bname, func.count("*")).join(Reservation).group_by(Boat.bid).order_by(desc(func.count("*"))).limit(1)
	
	assert query_check(orm_query, strsql)
	
def test_5():
	strsql = (
		"SELECT DISTINCT sailors.sid as sid, sailors.sname "
		"FROM sailors, boats, reserves "
		"WHERE sailors.sid = reserves.sid "
		"AND reserves.bid = boats.bid "
		"AND boats.color != \"red\" "
		"AND sailors.sid NOT IN ( SELECT sailors.sid "
						"FROM sailors, boats, reserves "
						"WHERE sailors.sid = reserves.sid "
						"AND boats.bid = reserves.bid "
						"AND boats.color = \"red\" ) "
		"UNION "
		"SELECT DISTINCT sailors.sid, sailors.sname "
		"FROM sailors, reserves "
		"WHERE sailors.sid NOT IN ( SELECT reserves.sid from reserves) "
		"ORDER BY sid;"
	)
	sailorAlias = aliased(Sailor)
	orm_query = s.query(Sailor.sid.label("sid"), Sailor.sname)\
					.join(Reservation)\
					.filter(Reservation.bid == Boat.bid)\
					.filter(Boat.color != "red")\
					.filter(Sailor.sid.notin_(
						s.query(sailorAlias.sid)\
							.join(Reservation)\
							.join(Boat)\
							.filter(Boat.color == "red")\
						)
					)\
					.union(s.query(Sailor.sid, Sailor.sname)\
							.filter(Sailor.sid.notin_(s.query(Reservation.sid)))
					)\
					.order_by("sid")
	assert query_check(orm_query, strsql)
	
	
def test_6():
	strsql = (
		"SELECT AVG(sailors.age) "
		"FROM sailors "
		"WHERE sailors.rating = 10;"
	)
	
	orm_query = s.query(func.avg(Sailor.age)).filter(Sailor.rating == 10)
	
	assert query_check(orm_query, strsql)
	
	
def test_7():
	strsql = (
		"SELECT sailors.sid, sailors.sname, sailors.rating, MIN(sailors.age) "
		"FROM sailors "
		"GROUP BY sailors.rating "
		"ORDER BY sailors.rating;"
	)
	
	orm_query = s.query(Sailor.sid, Sailor.sname, Sailor.rating, func.min(Sailor.age))\
					.group_by(Sailor.rating)\
					.order_by(Sailor.rating)
	assert query_check(orm_query, strsql)
	
def test_8():
	strsql = (
		"SELECT c.bid, c.sid, c.sname, MAX(num_reservations) "
		"FROM ( SELECT sailors.sname, reserves.sid, reserves.bid,  COUNT(reserves.bid) as num_reservations "
				"FROM sailors, reserves "
				"WHERE sailors.sid = reserves.sid "
				"GROUP BY reserves.bid, reserves.sid "
				"ORDER BY num_reservations DESC "
			") as c "
		"GROUP BY c.bid "
		"ORDER BY c.bid;"
	)
	
	subquery = s.query(Sailor.sname, Reservation.sid, Reservation.bid, func.count(Reservation.bid).label("num_reservations"))\
					.filter(Reservation.sid == Sailor.sid)\
					.group_by(Reservation.bid, Reservation.sid)\
					.order_by(desc("num_reservations"))\
					.subquery()
	orm_query = s.query(subquery.c.bid, subquery.c.sid, subquery.c.sname, func.max("num_reservations"))\
					.group_by(subquery.c.bid)\
					.order_by(subquery.c.bid)
	
	query_check(orm_query, strsql)



	
	
def get_daily_profit(session, date):
	'''
	Get the total daily profit (revenue - cost)
	'''
	daily_profit = get_daily_revenue(session, date) - get_daily_costs(session, date)
	return daily_profit
	
def get_daily_revenue(session, date):
	'''
	Get the daily revenue for all of the boats that were rented on the given date
	'''
	query = session.query(Boat.daily_price).filter(Boat.bid == Reservation.bid).filter(Reservation.day == date)
	daily_revenue = 0
	for record in query:
		daily_revenue += record[0]

	return daily_revenue

def get_daily_costs(session, date):
	'''
	Get the total costs (employees and boat maintainence) for the given day
	'''
	daily_costs = get_daily_boat_costs(session, date) + get_daily_employee_costs(session, date)
	return daily_costs
	
def get_daily_boat_costs(session, date):
	'''
	Get the total maintainence costs for all of the boats reserved on a given date
	'''
	query = session.query(Boat.daily_cost).filter(Boat.bid == Reservation.bid).filter(Reservation.day == date)
	daily_boat_costs = 0
	for record in query:
		daily_boat_costs += record[0]
	
	return daily_boat_costs
	
def get_daily_employee_costs(session, date):
	'''
	Get the total cost of employee wages for a given day
	'''
	query = session.query(Employee.hourly_wage, Shift.shift_end, Shift.shift_start)\
		.filter(Shift.eid == Employee.eid)\
		.filter(func.date(Shift.shift_start) == date)
		
	daily_employee_costs = 0
	for record in query:
		daily_employee_costs += record[0] * (record[1] - record[2]).seconds/3600

	return daily_employee_costs

	
def change_hourly_wage(session, employee_id, new_wage):
	'''
	Change a given employee's wage
	'''
	orig_wage = session.query(Employee.hourly_wage).filter(Employee.eid == employee_id)[0][0]
	session.query(Employee).filter(Employee.eid == employee_id).update({"hourly_wage": new_wage})
	session.commit()
	
	employee = session.query(Employee).filter(Employee.eid == employee_id)	
	return employee, orig_wage

def change_boat_price(session, bid, new_price):
	'''
	Change a given boat's price
	'''
	orig_price = session.query(Boat.daily_price).filter(Boat.bid == bid)[0][0]
	session.query(Boat).filter(Boat.bid == bid).update({"daily_price": new_price})
	session.commit()
	
	boat = session.query(Boat).filter(Boat.bid == bid)
	return boat, orig_price

def change_boat_cost(session, bid, new_cost):
	'''
	Change a given boat's price
	'''
	orig_cost = session.query(Boat.daily_cost).filter(Boat.bid == bid)[0][0]
	session.query(Boat).filter(Boat.bid == bid).update({"daily_cost": new_cost})
	session.commit()
	
	boat = session.query(Boat).filter(Boat.bid == bid)
	return boat, orig_cost
	

def test_change_price():
	boat, orig_price = change_boat_price(s, 102, 150)
	assert (boat[0].daily_price == 150) and (orig_price == 105)
	change_boat_price(s, 102, 105)
	
def test_change_cost():
	boat,  orig_cost = change_boat_cost(s, 102, 100)
	assert (boat[0].daily_cost == 100) and (orig_cost == 20)
	change_boat_cost(s, 102, 20)

def test_profits():
	
	assert get_daily_profit(s, date) == 695-386
	
	
def test_revenue():
	assert get_daily_revenue(s, date) == 695

def test_costs():
	assert get_daily_costs(s, date) == 386

def test_change_wage():
	employee, orig_wage = change_hourly_wage(s, 2, 15)
	assert  (employee[0].hourly_wage == Employee(eid=2, ename='ryan', hourly_wage=15).hourly_wage) and (orig_wage == 10)
	change_hourly_wage(s, 2, orig_wage)
