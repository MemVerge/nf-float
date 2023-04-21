params.str = 'Hello world from float and nextflow!'

process splitLetters {
  executor = 'float'
  image = 'cactus'
  cpu = 3
  mem = 6

  output:
    path 'chunk_*'

  """
  printf '${params.str}' | split -b 6 - chunk_
  """
}

process convertToUpper {
  executor = 'float'
  image = 'cactus'

  input:
    path x
  output:
    stdout

  """
  cat $x | tr '[a-z]' '[A-Z]'
  """
}

workflow {
  splitLetters | flatten | convertToUpper | view { it.trim() }
}
