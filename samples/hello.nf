process sayHello {
  executor = 'float'
  image = 'cactus'

  output:
    stdout

  """
  echo "Hello from NextFlow!"
  """
}

workflow {
  sayHello | view { it.trim() }
}
