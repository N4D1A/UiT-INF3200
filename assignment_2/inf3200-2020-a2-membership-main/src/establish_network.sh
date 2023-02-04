readarray -t hostsPortArray < hosts

for ((i=0; i < ${#hostsPortArray[@]}; ++i)); do
    curl -X POST "${hostsPortArray[$i]}/join?nprime=${hostsPortArray[0]}"
done