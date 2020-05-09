documentation https://www.python.org/dev/peps/pep-0257/

https://www.python.org/dev/peps/pep-0008/

pylint

http://superuser.com/questions/635690/convert-variables-between-camel-case-and-underscore-style

has sublime text tool for fixing code formate

classes and constructors
http://stackoverflow.com/questions/2825452/correct-approach-to-validate-attributes-of-an-instance-of-class

restricting to known variables
http://stackoverflow.com/questions/9646015/python-objects-avoiding-creation-of-attribute-with-unknown-name


#
# immutable
#
immutable object
from collections import namedtuple

Point = namedtuple("Point", "x, y")

a = Point(1,3)

print a.x, a.y
Because Point is now immutable your problem just can't happen, but the draw-back is naturally you can't e.g. just add +1 to a, but have to create a complete new Instance.

x,y = a
b = Point(x+1,y)

#
#
#
http://stackoverflow.com/questions/1319615/proper-way-to-declare-custom-exceptions-in-modern-python

https://scotch.io/tutorials/build-your-first-python-and-django-application

ORM
https://docs.djangoproject.com/en/1.10/topics/db/models/


https://docs.djangoproject.com/en/1.10/intro/tutorial02/

adding a user
https://docs.djangoproject.com/en/1.10/topics/auth/default/#user-objects

https://docs.djangoproject.com/en/1.10/topics/auth/default/#auth-admin`qu

after this is done
python manage.py createsuperuer

crud
https://rayed.com/wordpress/?p=1266

crudbuilder
https://github.com/asifpy/django-crudbuilder
https://www.google.com.ph/webhp?sourceid=chrome-instant&ion=1&espv=2&ie=UTF-8#q=django+crud+builder&*
https://docs.djangoproject.com/en/1.10/howto/legacy-databases/


https://django-crudbuilder.readthedocs.io/en/latest/installation.html
https://github.com/asifpy/django-crudbuilder/tree/master/example/example

--- forms ----

https://docs.djangoproject.com/en/1.8/topics/forms/

--- class based views ----
https://docs.djangoproject.com/en/1.10/topics/class-based-views/generic-editing/

---- mapping -----
http://docs.sqlalchemy.org/en/latest/orm/extensions/automap.html
looks like hibernate

---- models ----
https://docs.djangoproject.com/en/1.10/topics/db/models/


building all
http://stackoverflow.com/questions/29888046/django-1-8-create-initial-migrations-for-existing-schema

Finally got it to work, although I don't know why and I hope it will work in the future.
After doing numerous trials and going through Django's dev site (link).
Here are the steps (for whoever runs into this problem):

#Empty the django_migrations table:
delete from django_migrations;
#For every app, delete its migrations folder:
rm -rf <app>/migrations/
#Reset the migrations for the "built-in" apps:
python manage.py migrate --fake
#For each app run:
python manage.py makemigrations <app>.
#Take care of dependencies (models with ForeignKey's should run after their parent model).
Finally: python manage.py migrate --fake-initial
After that I ran the last command without the --fake-initial flag, just to make sure.


baker

http://stackoverflow.com/questions/10363581/how-to-create-views-automatically-from-model-as-in-django-admin

pip install django-baker

# oracle and python
http://www.oracle.com/technetwork/articles/dsl/prez-python-queries-101587.html

getting output as Decimal
https://community.oracle.com/thread/2538123

using unicode
http://www.programcreek.com/python/example/2699/cx_Oracle.NUMBER

lobs
http://stackoverflow.com/questions/8646968/how-do-i-read-cx-oracle-lob-data-in-python

more conversion
http://www.programcreek.com/python/example/2699/cx_Oracle.NUMBER

http://www.juliandyke.com/Research/Development/UsingPythonWithOracle.php

https://docs.python.org/2/library/functions.html

http://stackoverflow.com/question/372042/difference-between-abstract-class-and-interface-in-python

http://xlsxwriter.readthedocs.io/:

document
http://readthedocs.org/projects/xlsxwriter/search/

https://docs.python.org/2/library/pydoc.html
 In order to find objects and their documentation, pydoc imports the module(s) to be documented. Therefore, any code on module level will be executed on that occasion. Use an if __name__ == '__main__': guard to only execute code when a file is invoked as a script and not just imported.

Cleanup

http://stackoverflow.com/questions/865115/how-do-i-correctly-clean-up-a-python-object

crosstab
hrttp://pandas.pydata.org/pandas-docs/stable/generated/pandas.crosstab.html


tracing
python -m trace --trace module.py


###########
Installing on Fedora
#sudo postgresql-setup --initdb --unit postgresql
set -x
sudo systemctl enable postgresql
sudo systemctl start postgresql
sudo postgresql-setup --initdb --unit postgresql

#https://fedoramagazine.org/postgresql-quick-start-fedora-24/
#https://bbs.archlinux.org/viewtopic.php?id=149446
#https://fedoraproject.org/wiki/PostgreSQL#Installation

#http://stackoverflow.com/questions/34886981/postgresql-errors-install-and-run-postgresql-9-5
export LD_LIBRARY_PATH=/usr/lib/postgresql/9.5/lib:$LD_LIBRARY_PATH
sudo /sbin/ldconfig /usr/lib/postgresql/9.5/lib =

https://mkaz.tech/code/python-string-format-cookbook/

Version changes
https://pyformat.info/#string_pad_align

###
legacy database
https://docs.djangoproject.com/en/1.10/howto/legacy-databases/
https://pypi.python.org/pypi/django-baker/0.11

###
directory structure
http://python-guide-pt-br.readthedocs.io/en/latest/writing/structure/

##Types

from typing import Dict, Tuple, List

ConnectionOptions = Dict[str, str]
Address = Tuple[str, int]
Server = Tuple[Address, ConnectionOptions]

def broadcast_message(message: str, servers: List[Server]) -> None:
    ...

# The static type checker will treat the previous type signature as
# being exactly equivalent to this one.
def broadcast_message(
        message: str,
        servers: List[Tuple[Tuple[str, int], Dict[str, str]]]) -> None:

###
Integration Testing
https://www.fullstackpython.com/integration-testing.html


###
sphinx

Normally, there are no heading levels assigned to certain characters as the structure is determined from the succession of headings. However, this convention is used in Python’s Style Guide for documenting which you may follow:

# with overline, for parts
* with overline, for chapters
=, for sections
-, for subsections
^, for subsubsections
", for paragraphs

Data Structures
---------------

https://docs.python.org/2/library/collections.html#collections.namedtuple



http://machinelearningmastery.com/arima-for-time-series-forecasting-with-python/



https://pythontips.com/2013/07/30/20-python-libraries-you-cant-live-without/

http://stackoverflow.com/questions/10058140/accessing-items-in-a-ordereddict

logging
-------

https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/

pandas
------
http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html#pandas.DataFrame

http://stackoverflow.com/questions/28935315/how-do-i-display-a-pandas-dataframe-as-a-table-in-a-simple-kivy-app

http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html#pandas.DataFrame

python source
  os   what is ar

      warnings.warn(msg, DeprecationWarning, stacklevel=2)

def execle(file, *args):
    """execle(file, *args, env)

    Execute the executable file with argument list args and
    environment env, replacing the current process. """
    env = args[-1]
    execve(file, args[:-1], env)

 # Where Env Var Names Can Be Mixed Case
        class _Environ(UserDict.IterableUserDict):


"""Abstract Base Classes (ABCs) according to PEP 3119."""

from _weakrefset import WeakSet

look at antigravity.py xkcd

# Overwrite above definitions with a fast C implementation
try:
    from _bisect import *
except ImportError:
    pass


__all__ = ["IllegalMonthError", "IllegalWeekdayError", "setfirstweekday",
           "firstweekday", "isleap", "leapdays", "weekday", "monthrange",
           "monthcalendar", "prmonth", "month", "prcal", "calendar",
           "timegm", "month_name", "month_abbr", "day_name", "day_abbr"]

Cmd class for command interpreters

class Shape:
    def __init__(self, *, shapename, **kwds):
        self.shapename = shapename
        super().__init__(**kwds)


The lone * indicates that all following arguments are keyword-only arguments, that is, they can only be provided using their name, not as positional argument.

See PEP 3102 for further details.

def foo(a=[]):
    a.append(5)
    return a
Python novices would expect this function to always return a list with only one element: [5]. The result is instead very different, and very astonishing (for a novice):

>>> foo()
[5]
>>> foo()
[5, 5]
>>> foo()
[5, 5, 5]
>>> foo()
[5, 5, 5, 5]
>>> foo()def foo(a=[]):
    a.append(5)
    return a


def foo(first, *rest):
    if len(rest) > 1:
        raise TypeError("foo() expected at most 2 arguments, got %d"
                        % (len(rest) + 1))
    print 'first =', first
    if rest:
        print 'second =', rest[0]

http://stackoverflow.com/questions/4039879/best-way-to-find-the-months-between-two-dates

import datetime
from dateutil.rrule import rrule, MONTHLY

strt_dt = datetime.date(2001,1,1)
end_dt = datetime.date(2005,6,1)

dates = [dt for dt in rrule(MONTHLY, dtstart=strt_dt, until=end_dt)]

create new types
https://docs.python.org/3/library/typing.html
