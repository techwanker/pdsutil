
trim_trailing_spaces() {
	for f in *.py; 
	do     
    		sed  -i "s/ *$//" $f
	done
}

trim_trailing_spaces
