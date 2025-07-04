from sqlalchemy import DateTime, String, ForeignKey, Float
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from typing import List


class Base(AsyncAttrs, DeclarativeBase):
    __abstract__ = True

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)


class SymbolConfig(Base):
    __tablename__ = 'symbols_config'

    symbol_name: Mapped[str] = mapped_column(String(10), unique=True)
    grid_size: Mapped[float] = mapped_column(Float, default=0.1)
    init_grid_step: Mapped[float] = mapped_column(Float, default=0.0)
    lot: Mapped[float] = mapped_column(Float, default=1)


class Symbol(Base):
    __tablename__ = 'symbols'

    name: Mapped[str] = mapped_column(String(10), unique=True)
    # grid_size: Mapped[float] = mapped_column(Float)
    profit: Mapped[float] = mapped_column(Float, default=0.0)
    state: Mapped[str] = mapped_column(String(15), default='stop')

    orders: Mapped[List["OrderInfo"]] = relationship(back_populates="symbol")
    # grids: Mapped[List["VirtualGrid"]] = relationship(back_populates="symbol")


# class VirtualGrid(Base):
#     __tablename__ = 'virtual_grids'
#
#     first_grid_step: Mapped[float] = mapped_column(Float)
#     # tp_price: Mapped[float] = mapped_column(Float)
#     # symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id'), index=True)
#     # open_time: Mapped[DateTime] = mapped_column(DateTime)
#
#     symbol: Mapped["Symbol"] = relationship(back_populates="grids")

class OrderInfo(Base):
    __tablename__ = 'orders_info'

    price: Mapped[float] = mapped_column(Float)
    # executed_qty: Mapped[float] = mapped_column(Float)
    # cost: Mapped[float] = mapped_column(Float)
    # cost_with_fee: Mapped[float] = mapped_column(Float)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id'), index=True)
    open_time: Mapped[DateTime] = mapped_column(DateTime)

    symbol: Mapped["Symbol"] = relationship(back_populates="orders")
