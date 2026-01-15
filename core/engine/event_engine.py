import queue
import time
from typing import Dict, List, Callable
from .event import Event, EventType

class EventEngine:
    """
    事件引擎，负责事件的存储、分发和处理
    """
    def __init__(self):
        # 事件队列，使用 thread-safe 的 Queue 存储待处理事件
        self.__queue = queue.Queue()
        # 事件处理函数映射: {EventType: [handler1, handler2, ...]}
        # 存储每种事件类型对应的处理回调函数列表
        self.__handlers: Dict[EventType, List[Callable]] = {
            EventType.MARKET: [],
            EventType.SIGNAL: [],
            EventType.ORDER: [],
            EventType.FILL: []
        }
        # 引擎运行状态标志，True 表示正在运行
        self.__active = False

    def run(self):
        """
        运行事件引擎
        """
        self.__active = True
        while self.__active:
            try:
                # 获取事件，阻塞 1 秒
                event = self.__queue.get(block=True, timeout=1)
                self.__process(event)
            except queue.Empty:
                pass
            
    def stop(self):
        """
        停止事件引擎
        """
        self.__active = False

    def __process(self, event: Event):
        """
        处理事件
        """
        if event.type in self.__handlers:
            for handler in self.__handlers[event.type]:
                handler(event)

    def register(self, type: EventType, handler: Callable):
        """
        注册事件处理函数
        """
        if handler not in self.__handlers[type]:
            self.__handlers[type].append(handler)

    def unregister(self, type: EventType, handler: Callable):
        """
        注销事件处理函数
        """
        if handler in self.__handlers[type]:
            self.__handlers[type].remove(handler)

    def put(self, event: Event):
        """
        向队列中存入事件
        """
        self.__queue.put(event)

    def process_all_events(self):
        """
        处理队列中所有现有事件（非阻塞，用于回测）
        """
        while not self.__queue.empty():
            try:
                event = self.__queue.get(block=False)
                self.__process(event)
            except queue.Empty:
                break