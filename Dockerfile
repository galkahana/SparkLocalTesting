FROM python:3.10

# install poetry
RUN pip install poetry

# install java (for spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk;

# install app deps
WORKDIR /app
COPY poetry.lock pyproject.toml /app/
RUN poetry install

# copy source and run test
COPY . /app
RUN poetry run pytest .