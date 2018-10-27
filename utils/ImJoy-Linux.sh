#!/bin/bash
export PATH_BK=$PATH
export DEFAULT_PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
# detect conda in ImJoyEngine

export PATH=$HOME/ImJoyEngine/bin:$DEFAULT_PATH
condaPath=`which conda`

export PATH=$HOME/ImJoyEngine/bin:$PATH_BK:$DEFAULT_PATH
if [ "$condaPath" = "" ]; then
  if [ -d "$HOME/ImJoyEngine" ]; then
    DATE_WITH_TIME=`date "+%Y%m%d-%H%M%S"`
    mv "$HOME/ImJoyEngine" "$HOME/ImJoyEngine-$DATE_WITH_TIME"
  fi
  if [[ "$OSTYPE" == "linux-gnu" ]]; then
    # Linux
    bash ./Linux_Install.sh || bash ./ImJoy.app/Contents/Resources/Linux_Install.sh
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    # Mac OSX
    bash ./OSX_Install.sh || bash ./ImJoy.app/Contents/Resources/OSX_Install.sh
  elif [[ "$OSTYPE" == "freebsd"* ]]; then
    # ...
    bash ./Linux_Install.sh || bash ./ImJoy.app/Contents/Resources/Linux_Install.sh
  else
    echo "Unsupported OS."
  fi
  # detect conda in ImJoyEngine
  export PATH=$HOME/ImJoyEngine/bin:$DEFAULT_PATH
  condaPath=`which conda`
  if [ "$condaPath" = "" ]; then
    echo "Failed to install Miniconda for ImJoy."
  fi
  export PATH=$HOME/ImJoyEngine/bin:$PATH_BK:$DEFAULT_PATH
  $HOME/ImJoyEngine/bin/python -m imjoy "$@"
else
condaRoot=`dirname "$condaPath"`
$condaRoot/python -m imjoy "$@" || pip install git+https://github.com/oeway/ImJoy-Engine#egg=imjoy && $condaRoot/python -m imjoy "$@"
fi
