<div align="center">

# ArtometriX-Official

![Static Badge](https://img.shields.io/badge/python-3.9+-blue)
![Static Badge](https://img.shields.io/badge/code_style-black-black)
> Empowering Your Business with Data-Driven Strategies
</div>



> [!NOTE]
> We are adding new articles so often that you should update often as well. That means: `git pull develop` in the main directory!


## 📚 Blog article

- Marketing 

- Linear programming

- Mathematical fundations

## 🎬 Prerequisites

- python 3.10
- package manager pip or conda
- Docker: https://www.docker.com/get-started

## Quickstart 1

**Install dependencies**

```sh
pip install -r requirements.txt
```

**Launch Jupyter notebook**

```sh
jupyter lab --port=8888
```

## Quickstart 2

**build docker images**

```sh
docker compose build
```

make sure to change volume to match the path where the repository is located.

```sh
docker compose up 
```

Connect to remote compute 

```
ssh -L 8087:localhost:8087
```


```sh
docker run -it --rm -v $(pwd):/app arto_labs:latest /bin/bash
```



## Optimization 

```
python3 -m lib --pipeline='optimization_curves'
```
## License

This blog is licensed under the terms of the MIT license
