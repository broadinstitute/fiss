task echo {
	String echo_me

	command {
		echo  ${echo_me} && echo ${echo_me} > echo.txt
	}

	runtime { 
		docker: "broadgdac/firecloud-ubuntu:15.10"
	}

	output {
		File echoed="echo.txt"
	}
}