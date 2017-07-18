task echo_task {
	String message

	command {
		echo  ${message} && echo ${message} > echo.txt
	}

	runtime { 
		docker: "broadgdac/firecloud-ubuntu:15.10"
	}

	output {
		File echoed="echo.txt"
	}
}

workflow echo {
	call echo_task
}
