This is a project to visualize a running storm topology

## What does it do

It shows a visual representation of the volume of tuples sent from one storm component to the next

I stole most everything that makes this interesting. Credit is due to Nathan Marz and Michael Bostock in this regard.

Below is an image based on an expansion of the word count topology: (if I can work github's md syntax)

Inline-style:
![alt text](https://github.com/cfergus/StormSankey/raw/master/sankey-storm.png "Example Visualization of a Storm Topology")

## How does it suck?

Many ways!

This is my first project on:
* github
* clojure
* storm
* sankey

As such, there will be demons all about.

Of most significance:
* Only works on the 'default' stream
* Pinned to port 8080 instead of the UI port
* Probably won't work if you check it out and try to run it
* Requires you to hand-enter the topology id into the URL

Many of these deficiencies will be addressed depending on how it might be incorporated relevant to the native Storm UI

## How can it be made better?
I am interested in receiving feedback on:
* What clojure forms/syntax would be better to use. (there are many opportunities for feedback)
* In what ways would the visualization be more useful. No guarantees that they will be implemented.
* In what ways could this be a better github neighbor. Such as:
** License inclusion
** Should it instead be a fork of storm, with a pull request
** Have I caused any great offenses in my project structure or otherwise