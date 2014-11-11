# airpair-clj-spark-introduction

## Usage

Setup the following environment variables in your shell with your *own* values:

```
set -x DUCK_AUTH_TOKEN InWgapQLxFQhCXFt7hST
set -x APP_CONSUMER_KEY TQD9cjDFrA
set -x APP_CONSUMER_SECRET 3H05OVI80mFbQ6xO8FmH
set -x USER_ACCESS_TOKEN 12343123-tViU5KEmN3Xzu
set -x USER_ACCESS_TOKEN_SECRET eFLd7g3pnjFJEBAXK5sLincV
```

Mine the twitter data by running:
```
lein mine
```

Run the few manipulations and posting to Ducksboard:
```
lein run
```

## License

Copyright © 2014 Sébastien Arnaud

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
