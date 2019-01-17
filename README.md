##MovieLens
This is a example of how to write Scala Spark/Spark SQL using Test Driven Development.  

## Prerequisites
1. JDK 1.8    
2. Scala 2.11.8
3. SBT 
    * Tool for library dependency management. .
4. I've used Intellij IDEA for development and testing. If using Intellij then
    * Install following Plugin
        * SBT
        * Scala
        * (See build.sbt for component versions)

####Data Files 
Create following "::" delimiter files in /tmp dir

* Movie.dat::
```
21::Toy Story (1995)::Animation|Children's|Comedy
22::Jumanji (1995)::Adventure|Children's|Fantasy
23::Grumpier Old Men (1995)::Comedy|Romance
24::Waiting to Exhale (1995)::Comedy|Drama
25::Father of the Bride Part II (1995)::Comedy
```

* User.dat::
```
1::F::1::10::48067
2::M::56::16::70072
3::M::25::15::55117
4::M::45::7::02460
6::F::50::9::55117
```

* Rating.dat::
```
1::21::5::978300760
1::22::3::978300760
2::22::4::978299026
2::23::5::978299026
2::24::4::978299026
2::25::3::978299026
3::23::2::978297837
4::654321::5::978294008
5::25::3::978245037
```

#### Running Test
* movielens (master)*$ sbt clean package test

#### Acknowledgments

* Not Handled exception like Bad rows.

## Change Log
* 0.0.1
   * Initial Commit

## Meta
https://github.com/afzals2000/SparkUsingTDD

## Contributing
1. Fork it (https://github.com/afzals2000/SparkUsingTDD)
2. Create your feature branch (git checkout -b feature/fooBar)
3. Commit your changes (git commit -am 'Add some fooBar')
4. Push to the branch (git push origin feature/fooBar)
5. Create a new Pull Request