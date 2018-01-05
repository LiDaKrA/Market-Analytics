Preparations and Preprocessing
---------------------------------------
1. Download and unpack data
---------------------------------------
On Linux use either transmission or rtorrent 
(sudo apt-get install transmission-cli)
transmission-cli https://archive.org/download/dnmarchives/dnmarchives_archive.torrent

(sudo apt-get install rtorrent)
rtorrent
# Press enter to get to the bottom
# Paste https://archive.org/download/dnmarchives/dnmarchives_archive.torrent
# Press up or down to mark the entry
# Press Ctrl + O to change download directory
# Press Ctrl + S to start the download

# To unpack the data for a specific pattern cd into the directory and execute
for file in *agora*.tar.xz; do tar -v -x -f $file -C targetdirectory; done

2. Rename Files on Linux
---------------------------------------
# Certain characters get converted automatically on Windows. To align them, run the following for all unpacked directories
find . -type f -name "*?*" -exec rename "s/\?/_/" "{}" + ;

3. Setup databases
---------------------------------------
sudo -u username createdb Dogeroad-Forum
sudo -u username createdb Dogeroad-Marketplace
sudo -u username createdb Agora-Marketplace
sudo -u username createdb Silkroad2-Forum
sudo -u username createdb Silkroad2-Marketplace
