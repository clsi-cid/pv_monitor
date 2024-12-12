# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
from logging import *


class PVMonLogger(Logger):
    """
    A custom logger inheriting from Python's logging module.

    This logger allows including stack information in log messages globally
    by setting the stack_info attribute.

    Attributes:
        stack_info (bool): Flag to globally include stack information in log
        messages.

    Methods:
        setStackInfo(stack_info):
            Set whether to globally include stack information in log messages.

        debug(msg, *args, **kwargs):
            Log a message with severity 'DEBUG'.
            If the stack_info argument is provided,
            it will use the one passed in for this log message.

        info(msg, *args, **kwargs):
            Log a message with severity 'INFO'.
            If the stack_info argument is provided,
            it will use the one passed in for this log message.

        warning(msg, *args, **kwargs):
            Log a message with severity 'WARNING'.
            If the stack_info argument is provided,
            it will use the one passed in for this log message.

        error(msg, *args, **kwargs):
            Log a message with severity 'ERROR'.
            If the stack_info argument is provided,
            it will use the one passed in for this log message.

        critical(msg, *args, **kwargs):
            Log a message with severity 'CRITICAL'.
            If the stack_info argument is provided,
            it will use the one passed in for this log message.

        exception(msg, *args, stack_info=True, exc_info=True, **kwargs):
            Convenience method to log an ERROR with exception information.
            stack_info for exception is default to True.
    """

    def __init__(self, name, level=NOTSET):
        self.stack_info = False
        return super(PVMonLogger, self).__init__(name, level)

    def setStackInfo(self, stack_info):
        """
        Set whether to include stack information in log messages.

        Args:
            stack_info (bool): Flag to include stack information.
        """
        self.stack_info = stack_info

    def debug(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'DEBUG'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.debug("Houston, we have a %s", "thorny problem", exc_info=1)
        """
        if self.isEnabledFor(DEBUG):
            if kwargs.get("stack_info") is not None:
                super(PVMonLogger, self).debug(msg, *args, **kwargs)
            else:
                super(PVMonLogger, self).debug(
                    msg, *args, stack_info=self.stack_info, **kwargs
                )

    def info(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'INFO'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.info("Houston, we have a %s", "interesting problem", exc_info=1)
        """
        if self.isEnabledFor(INFO):
            if kwargs.get("stack_info") is not None:
                super(PVMonLogger, self).info(msg, *args, **kwargs)
            else:
                super(PVMonLogger, self).info(
                    msg, *args, stack_info=self.stack_info, **kwargs
                )

    def warning(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'WARNING'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.warning("Houston, we have a %s", "bit of a problem", exc_info=1)
        """
        if self.isEnabledFor(WARNING):
            if kwargs.get("stack_info") is not None:
                super(PVMonLogger, self).warning(msg, *args, **kwargs)
            else:
                super(PVMonLogger, self).warning(
                    msg, *args, stack_info=self.stack_info, **kwargs
                )

    def error(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'ERROR'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.error("Houston, we have a %s", "major problem", exc_info=1)
        """
        if self.isEnabledFor(ERROR):
            if kwargs.get("stack_info") is not None:
                super(PVMonLogger, self).error(msg, *args, **kwargs)
            else:
                super(PVMonLogger, self).error(
                    msg, *args, stack_info=self.stack_info, **kwargs
                )

    def critical(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'CRITICAL'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.critical("Houston, we have a %s", "major disaster", exc_info=1)
        """
        if self.isEnabledFor(CRITICAL):
            if kwargs.get("stack_info") is not None:
                super(PVMonLogger, self).critical(msg, *args, **kwargs)
            else:
                super(PVMonLogger, self).critical(
                    msg, *args, stack_info=self.stack_info, **kwargs
                )

    def exception(self, msg, *args, stack_info=True, exc_info=True, **kwargs):
        """
        Convenience method for logging an ERROR with exception information.
        """
        self.error(
            msg, *args, stack_info=stack_info, exc_info=exc_info, **kwargs
        )
