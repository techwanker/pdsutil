set -x
git init
git config --global user.name "Jim Schmidt"
git config --global user.email james.joseph.schmidt+pds@gmail.com
git remote add origin https://sea-ds@bitbucket.org/sea-ds/pdsutil.git
git add -A *
git commit
git push -u origin master

