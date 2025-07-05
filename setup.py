from setuptools import setup, find_packages

setup(
    name='fitanalysis',
    version='0.1.0',
    author='OpenHands',
    author_email='openhands@all-hands.dev',
    description='Library to load and analyze Garmin FIT files',
    packages=find_packages(),
    install_requires=[
        'fitparse>=1.1.0',
        'pandas>=1.0.0',
    ],
    extras_require={
        'web': [
            'fastapi>=0.100.0',
            'uvicorn>=0.22.0',
            'python-multipart>=0.0.6',
            'httpx>=0.24.0',  # For FastAPI test client
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
    ],
)
