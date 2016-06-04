from setuptools import setup

tests_require = [
    'mock',
    'testtools'
]

setup(
    name='SpreadFlowCore',
    version='0.0.1',
    description='Metadata extraction and processing engine',
    author='Lorenz Schori',
    author_email='lo@znerol.ch',
    url='https://github.com/znerol/spreadflow-core',
    packages=[
        'spreadflow_core',
        'spreadflow_core.scripts',
        'spreadflow_core.test',
        'twisted.plugins'
    ],
    package_data={
        'twisted.plugins': [
            'twisted/plugins/spreadflow_core_service.py',
        ]
    },
    entry_points={
        'console_scripts': [
            'spreadflow-twistd = spreadflow_core.scripts.spreadflow_twistd:main',
        ]
    },
    install_requires=[
        'Twisted',
        'toposort',
        'zope.interface'
    ],
    tests_require=tests_require,
    extras_require={
        'tests': tests_require
    },
    zip_safe=False,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Framework :: Twisted',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Topic :: Multimedia'
    ],
)
