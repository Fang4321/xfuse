from setuptools import setup, find_packages

setup(
    name='torrentfs',
    version='0.1',
    description='A FUSE-based torrent filesystem',
    author='Fang4321',
    author_email='fangkunliangg@gmail.com',
    packages=find_packages(),
    install_requires=[
        'bencodepy>=0.9.5',
        'fusepy>=3.0.1',
    ],
    entry_points={
        'console_scripts': [
            'xfuse = xfuse.main:main',
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: POSIX :: Linux',
        'License :: OSI Approved :: MIT License',
    ],
    python_requires='>=3.6',
)
