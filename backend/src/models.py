from sqlalchemy import Column,Integer,String,Float,Boolean,ForeignKey,JSON,UniqueConstraint,CheckConstraint
from .db import Base
from .constants import SAMPLE_STATES,LOT_STATES
from .artifacts import artifact_matches
class User(Base):
    __tablename__='users'
    id=Column(Integer,primary_key=True);email=Column(String(100),unique=True,nullable=False);password_hash=Column(String(220),nullable=False);role=Column(String(30),nullable=False)
class Audit(Base):
    __tablename__='audits'
    id=Column(Integer,primary_key=True);actor=Column(String(100));action=Column(String(300))
class Clock(Base):
    __tablename__='clock'
    id=Column(Integer,primary_key=True);now=Column(String(20),nullable=False)
class Consignment(Base):
    __tablename__='consignments'
    id=Column(Integer,primary_key=True);code=Column(String(60),unique=True,nullable=False);grower=Column(String(100),nullable=False);variety=Column(String(80),nullable=False);cut_at=Column(String(20),nullable=False);arrived_at=Column(String(20),nullable=False);crates=Column(Integer,nullable=False);stems_per_crate=Column(Integer,nullable=False);note=Column(String(250),default='')
    __table_args__=(CheckConstraint('crates>0 AND stems_per_crate>0'),)
class VaseSample(Base):
    __tablename__='vase_samples'
    id=Column(Integer,primary_key=True);consignment_id=Column(Integer,ForeignKey('consignments.id'),nullable=False);code=Column(String(60),unique=True,nullable=False);maturity=Column(Float,nullable=False);stem_cm=Column(Float,nullable=False);cold_hours=Column(Float,nullable=False);observed_days=Column(Float,nullable=True);state=Column(String(20),default=SAMPLE_STATES[0]);prediction=Column(JSON);error=Column(String(450));approved_grade=Column(String(2));review_note=Column(String(250))
    # 可靠估级流水线：compute_generation 标识当前这一代计算，入队/补算时 +1；
    # 晚回来的旧计算代际对不上，结果作废，不覆盖已补好的估值与归档。
    compute_generation=Column(Integer,nullable=False,default=1,server_default='1')
    # 入队与 worker 心跳时间（业务无关的墙钟），恢复器据此识别丢消息/硬崩溃的单子。
    enqueued_at=Column(String(30),nullable=True);heartbeat_at=Column(String(30),nullable=True)

    @property
    def artifact_ready(self):
        """估值与归档齐了、且归档与当前代际同一次计算，才可交主管核级。"""
        return self.state=='ready' and self.prediction is not None and artifact_matches(self)
class AuctionSession(Base):
    __tablename__='auction_sessions'
    id=Column(Integer,primary_key=True);code=Column(String(60),unique=True,nullable=False);opens_at=Column(String(20),nullable=False);closes_at=Column(String(20),nullable=False)
class AuctionLot(Base):
    __tablename__='auction_lots'
    id=Column(Integer,primary_key=True);code=Column(String(60),unique=True,nullable=False);consignment_id=Column(Integer,ForeignKey('consignments.id'),nullable=False);sample_id=Column(Integer,ForeignKey('vase_samples.id'),nullable=False);session_id=Column(Integer,ForeignKey('auction_sessions.id'),nullable=False);crates=Column(Integer,nullable=False);reserve_cents=Column(Integer,nullable=False);state=Column(String(20),default=LOT_STATES[0]);grade_snapshot=Column(String(2),nullable=False)
    __table_args__=(CheckConstraint('crates>0 AND reserve_cents>0'),)
class Bid(Base):
    __tablename__='bids'
    id=Column(Integer,primary_key=True);lot_id=Column(Integer,ForeignKey('auction_lots.id'),nullable=False);buyer=Column(String(80),nullable=False);unit_cents=Column(Integer,nullable=False);at=Column(String(20),nullable=False)
class Deal(Base):
    __tablename__='deals'
    id=Column(Integer,primary_key=True);lot_id=Column(Integer,ForeignKey('auction_lots.id'),unique=True,nullable=False);bid_id=Column(Integer,ForeignKey('bids.id'),nullable=False);buyer=Column(String(80),nullable=False);crates=Column(Integer,nullable=False);unit_cents=Column(Integer,nullable=False);original_grade=Column(String(2),nullable=False);effective_grade=Column(String(2),nullable=False);revision=Column(Integer,default=1);at=Column(String(20),nullable=False)
class Dispute(Base):
    __tablename__='disputes'
    id=Column(Integer,primary_key=True);deal_id=Column(Integer,ForeignKey('deals.id'),nullable=False);reason=Column(String(250),nullable=False);state=Column(String(20),default='opened');decision_note=Column(String(250));revised_grade=Column(String(2));sample_snapshot=Column(JSON)
class Settlement(Base):
    __tablename__='settlements'
    id=Column(Integer,primary_key=True);deal_id=Column(Integer,ForeignKey('deals.id'),nullable=False);revision=Column(Integer,nullable=False);kind=Column(String(20),nullable=False);gross_cents=Column(Integer,nullable=False);commission_cents=Column(Integer,nullable=False);target_cents=Column(Integer,nullable=False);delta_cents=Column(Integer,nullable=False);state=Column(String(20),default='draft');snapshot=Column(JSON);receipt=Column(String(80))
    __table_args__=(UniqueConstraint('deal_id','revision'),)
