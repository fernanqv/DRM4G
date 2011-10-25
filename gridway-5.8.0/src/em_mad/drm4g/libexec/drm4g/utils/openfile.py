import re

__version__ = '0.1'
__author__  = 'Carlos Blanco'
__revision__ = "$Id$"


comment      = re.compile(r'#.*$')
line_comment = re.compile(r'^#+')

def cleaner(filename):
    """
    cleaner reads a file and returns a string without comments.
    Syntax of comment:
       # Comment
    """
    try: 
        f = open(filename, 'r')
    except IOError:
        print 'Cannot open ', path
    else:
        lines = f.readlines()
        f.close()
        return ''.join([re.sub(comment, '', line) for line in lines if not line_comment.search(line)]).rstrip()
