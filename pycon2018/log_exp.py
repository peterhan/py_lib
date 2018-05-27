import os
import sys
import logging
import logging.config
import pdb


DICT={
    "version":1,
    "formatters":{
        "simple":{
            "format":"%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },
         "complex":{
            "format":"format=%(asctime)s - %(name)s - %(levelname)s - %(module)s : %(lineno)d - %(message)s"
         }
    },
    "handlers":{
        "file":{
            "class":"logging.handlers.TimedRotatingFileHandler",
            "interval":3600,
            "backupCount":"5",
            "formatter":"complex",
            "level":"DEBUG",
            "filename":"testSuite.log"
        },
        "console":{
            "class":"logging.StreamHandler",
            "formatter":"simple",
            "level":"DEBUG",
            }    
    },
    "root":{
        "handlers":["console"],
        "level":"DEBUG"
    } ,
    "loggers":{        
        "sub":{
            "handlers":["console"],
            "level":"DEBUG"
            }    
    }
}
class Main(object):
  @staticmethod
  def main():
    # logging.config.fileConfig("logging.conf")
    logging.config.dictConfig(DICT)
    sub_log=logging.getLogger('sub')
    # pdb.set_trace()
    try:raise Exception
    except:sub_log.debug('from sub',exc_info=True)
    logging.debug("1")
    logging.info("2")
    logging.warn("3")
    logging.error("4")
    logging.critical("5")

if __name__ == "__main__":
  Main.main()