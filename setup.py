from setuptools import setup
import os

root = os.path.dirname(os.path.abspath(__file__))

about = {}
with open(os.path.join(root, 'asched', '__version__.py'), 'r') as f:
    exec(f.read(), about)

requirements = []
with open(os.path.join(root, 'requirements.txt')) as f:
    requirements = f.read().splitlines()

readme = ''
with open(os.path.join(root, 'README.md')) as f:
    readme = f.read()

setup(name=about['__title__'],
      author=about['__author__'],
      author_email='ishlyakhov@gmail.com',
      url='http://github.com/isanich/ached',
      version=about['__version__'],
      license=about['__license__'],
      description='asched can schedule your asyncio coroutines for a specific time or interval,'
                  ' keep their run stats and reschedule them from the last state when restarted.',
      long_description=readme,
      dependency_links=['git+https://github.com/iissme/asyncio-mongo-reflection/.git@master'
                        '#egg=asyncio-mongo-reflection-0.0.1'],
      packages=['asched'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest'],
      include_package_data=True,
      install_requires=requirements,
      platforms='any',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: MIT License',
          'Intended Audience :: Developers',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3.6',
          'Topic :: Software Development :: Libraries',
          'Topic :: Software Development :: Libraries :: Python Modules',
      ]
      )
