import logging

def counted(fn):
	def wrapper(*args, **kwargs):
		wrapper.count += 1
		return fn(*args, **kwargs)
	wrapper.count = 0
	wrapper.__name__ = fn.__name__
	return wrapper


class MyLogger(logging.Logger):
	def __init__(self, name=None, level=logging.NOTSET):
		super(MyLogger, self).__init__(name, level)

	@counted
	def info(self, *args, **kwargs):
			super(MyLogger, self).info(*args, **kwargs)

	@counted
	def warning(self, *args, **kwargs):
		super(MyLogger, self).warning(*args, **kwargs)

	@counted
	def critical(self, msg, *args, **kwargs):
		super(MyLogger, self).critical(*args, **kwargs)

	@counted
	def error(self, *args, **kwargs):
		super(MyLogger, self).error(*args, **kwargs)

	def logfile(self):
		for h in self.handlers:
			if hasattr(h, 'baseFilename'):
				return h.baseFilename

	def empty(self):
		if self.warning.count or self.critical.count or self.error.count:
			return False
		else:
			return True

	def status(self):
		msg = "WARNINGS:%s ERRORS:%s CRITICAL:%s" % (self.warning.count, self.error.count, self.critical.count)
		return msg


def addLogFile(logger, filepath, level):
	handler = logging.FileHandler(filepath, "a", encoding=None, delay="true")
	handler.setLevel(level)
	formatter = logging.Formatter("'%(asctime)s [%(name)s] : [%(levelname)s] : %(message)s'")
	handler.setFormatter(formatter)
	logger.addHandler(handler)

def addLogConsole(logger, level):
	handler = logging.StreamHandler()
	handler.setLevel(level)
	formatter = logging.Formatter("'%(asctime)s [%(name)s] : [%(levelname)s] : %(message)s'")
	handler.setFormatter(formatter)
	logger.addHandler(handler)

def myLog(level=None):
	if not LOGGER.handlers:
		# "Adding Handlers..."
		addLogConsole(LOGGER)
		addLogFile(LOGGER, '#YOUR LOG FILE#')
	return LOGGER