:inicio
	taskkill /IM python_jobs.exe
	cd C:\@work\pessoal\Criptomante_python
	python_jobs job.py
	timeout /t 300
	goto :inicio