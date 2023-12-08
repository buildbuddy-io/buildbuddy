free -h

python3 -c "
import time
arr = str(1) * 6760000000
time.sleep(10)
" &
disown
sleep 3

free -h
ps