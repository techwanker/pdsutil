Installation
============

Operating System Prerequisites
------------------------------


On your linux host run

sudo dnf install docker
or
sudo apt-get install docker

Configure Docker
----------------

Installing on debian based and redhat based linux distros

https://www.jamescoyle.net/how-to/1503-create-your-first-docker-container

Install docker
^^^^^^^^^^^^^^
from the bash shell run the following::

    docker pull ubuntu
    docker images

This will show you your images::

    jjs@lenovo$ **docker images**
    REPOSITORY            TAG                   IMAGE ID            CREATED             SIZE
    ubuntu                latest                f7b3f317ec73        6 days ago          117MB
    openchain_openchain   latest                ac6f6ad5c898        3 months ago        755MB
    microsoft/dotnet      1.1-sdk-projectjson   3de737a2226e        3 months ago        581MB
    ubuntu                <none>                f49eec89601e        3 months ago        129MB
    hello-world           latest                48b5124b2768        3 months ago        1.84kB
    centos                latest                67591570dd29        4 months ago        192MB
    fedora                latest                a1e614f0f30e        4 months ago        197MB

    Now to install the latest Ubuntu

    jjs@lenovo-ubuntu$ **docker run -i -t f7b3f317ec73  /bin/bash**
root@24e491203045:/#

::

    docker ps -a

Install operating system prerequisites
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

http://stackoverflow.com/questions/27273412/cannot-install-packages-inside-docker-ubuntu-image

per https://docs.docker.com/engine/userguide/eng-image/dockerfile_best-practices/#run
Always combine RUN apt-get update with apt-get install in the same RUN statement, for example

::

    apt-get update && apt-get install \
        python3-pip \
        postgresql-9.5 \
        libssl-dev \
        python3-setuptools \
        docker \
        git \
        vim

Note

Install python packages
^^^^^^^^^^^^^^^^^^^^^^^
You may wish to install the python packages required so that they are in your docker image and they don't have to be
downloaded every time.  This allows me to fire up a docker image and work offline if I ssh into my container.


Commit the docker container
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Inside your host, not within the docker container

::

     docker commit dee8bc7ed264 ubuntu-python35:no_python_pkg

The second argument should be after the @ in your docker prompt

Pull down the projects
----------------------
from the shell::

    mkdir pds
    cd pds
    git clone https://sea-ds@bitbucket.org/sea-ds/pdsutil.git

Download python package requirements
------------------------------------
from the shell::

    cd /pds/pdsutil
    make init

Run the tests
-------------
from the shell::

    python3.5 setup.py test

Install the package