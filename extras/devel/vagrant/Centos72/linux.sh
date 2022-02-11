# WITHOUT CONDA

# Install python3
sudo yum install -y python3 gcc python3-devel

# Update pip3 
sudo pip3 install --upgrade pip

# Install wrf4g 
pip3 install drm4g --user

# WITH CONDA
#sudo yum install -y wget gcc
#wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
#chmod +x miniconda.sh
#./miniconda.sh -b -p /home/vagrant/miniconda
#export PATH="/home/vagrant/miniconda/bin:$PATH"
#conda create -n wrf4g-py36 python=3.6
#conda init bash
#conda activate wrf4g-py36

# INSTALL FROM GIT
#git clone https://github.com/SantanderMetGroup/WRF4G.git
#sudo yum install -y git
#git clone https://github.com/fernanqv/WRF4G.git
#cd WRF4G
#git checkout wrf-cmake
#pip install -e .

# Test wrf4g
#wrf4g start
#wrf4g resource
#wrf4g exp test define --from-template=single
#wrf4g exp test create --dir test
#wrf4g exp test submit






