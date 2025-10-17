conda deactivate
conda create -p ./maternoscope_env python=3.14 --yes
conda activate ./maternoscope_env
conda install conda-forge::requests --yes
pip install python-dotenv
conda install conda-forge::schedule --yes
conda install conda-forge::praw --yes
conda install conda-forge::pandas --yes
