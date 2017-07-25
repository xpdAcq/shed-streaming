# class partition(EventStream):
#     def __init__(self, n, child):
#         self.n = n
#         self.buffer = []
#         EventStream.__init__(self, child)
#
#     def update(self, x, who=None):
#         self.buffer.append(x)
#         if len(self.buffer) == self.n:
#             result, self.buffer = self.buffer, []
#             return self.emit(tuple(result))
#         else:
#             return []


# class sliding_window(EventStream):
#     def __init__(self, n, child):
#         self.n = n
#         self.buffer = deque(maxlen=n)
#         EventStream.__init__(self, child)
#
#     def update(self, x, who=None):
#         self.buffer.append(x)
#         if len(self.buffer) == self.n:
#             return self.emit(tuple(self.buffer))
#         else:
#             return []


# class timed_window(EventStream):
#     def __init__(self, interval, child, loop=None):
#         self.interval = interval
#         self.buffer = []
#         self.last = gen.moment
#
#         EventStream.__init__(self, child, loop=loop)
#
#         self.loop.add_callback(self.cb)
#
#     def update(self, x, who=None):
#         self.buffer.append(x)
#         return self.last
#
#     @gen.coroutine
#     def cb(self):
#         while True:
#             L, self.buffer = self.buffer, []
#             self.last = self.emit(L)
#             yield self.last
#             yield gen.sleep(self.interval)


# class delay(EventStream):
#     def __init__(self, interval, child, loop=None):
#         self.interval = interval
#         self.queue = Queue()
#
#         EventStream.__init__(self, child, loop=loop)
#
#         self.loop.add_callback(self.cb)
#
#     @gen.coroutine
#     def cb(self):
#         while True:
#             last = time()
#             x = yield self.queue.get()
#             yield self.emit(x)
#             duration = self.interval - (time() - last)
#             if duration > 0:
#                 yield gen.sleep(duration)
#
#     def update(self, x, who=None):
#         return self.queue.put(x)


# class rate_limit(EventStream):
#     def __init__(self, interval, child):
#         self.interval = interval
#         self.next = 0
#
#         EventStream.__init__(self, child)
#
#     @gen.coroutine
#     def update(self, x, who=None):
#         now = time()
#         old_next = self.next
#         self.next = max(now, self.next) + self.interval
#         if now < old_next:
#             yield gen.sleep(old_next - now)
#         yield self.emit(x)


# class buffer(EventStream):
#     def __init__(self, n, child, loop=None):
#         self.queue = Queue(maxsize=n)
#
#         EventStream.__init__(self, child, loop=loop)
#
#         self.loop.add_callback(self.cb)
#
#     def update(self, x, who=None):
#         return self.queue.put(x)
#
#     @gen.coroutine
#     def cb(self):
#         while True:
#             x = yield self.queue.get()
#             yield self.emit(x)

# class concat(EventStream):
#     def update(self, x, who=None):
#         L = []
#         for item in x:
#             y = self.emit(item)
#             if type(y) is list:
#                 L.extend(y)
#             else:
#                 L.append(y)
#         return L


# class unique(EventStream):
#     def __init__(self, child, history=None, key=identity):
#         self.seen = dict()
#         self.key = key
#         if history:
#             from zict import LRU
#             self.seen = LRU(history, self.seen)
#
#         EventStream.__init__(self, child)
#
#     def update(self, x, who=None):
#         y = self.key(x)
#         if y not in self.seen:
#             self.seen[y] = 1
#             return self.emit(x)


# class union(EventStream):
#     def update(self, x, who=None):
#         return self.emit(x)


# class collect(EventStream):
#     def __init__(self, child, cache=None):
#         if cache is None:
#             cache = deque()
#         self.cache = cache
#
#         EventStream.__init__(self, child)
#
#     def update(self, x, who=None):
#         self.cache.append(x)
#
#     def flush(self, _=None):
#         out = tuple(self.cache)
#         self.emit(out)
#         self.cache.clear()
