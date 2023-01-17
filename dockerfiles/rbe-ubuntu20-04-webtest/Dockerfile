FROM gcr.io/flame-public/rbe-ubuntu20-04@sha256:036ae8c90876fa22da9ace6f8218e614f4cd500a154fc162973fff691e72d28e

# Install debs needed for Chrome but not Chrome itself, since webdriver tests
# will include specific Chrome versions in Bazel runfiles but rely on certain
# system libs being present such as libgl.
#
# This list was retrieved by installing google-chrome and then copying the
# package listing after "the following additional packages will be installed".
#
# Run this command in Ubuntu 20.04 to see that listing:
#
#     cd $(mktemp -d) && \
#     wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
#     apt-get update && \
#     apt install -y ./google-chrome-stable_current_amd64.deb

RUN apt-get update && \
    apt-get install -y \
      adwaita-icon-theme \
      at-spi2-core \
      dconf-gsettings-backend \
      dconf-service \
      fontconfig \
      fonts-liberation \
      glib-networking \
      glib-networking-common \
      glib-networking-services \
      gsettings-desktop-schemas \
      gtk-update-icon-cache \
      hicolor-icon-theme \
      humanity-icon-theme \
      libatk-bridge2.0-0 \
      libatk1.0-0 \
      libatk1.0-data \
      libatspi2.0-0 \
      libauthen-sasl-perl \
      libcairo-gobject2 \
      libcairo2 \
      libcolord2 \
      libdata-dump-perl \
      libdatrie1 \
      libdconf1 \
      libdrm-amdgpu1 \
      libdrm-common \
      libdrm-intel1 \
      libdrm-nouveau2 \
      libdrm-radeon1 \
      libdrm2 \
      libelf1 \
      libencode-locale-perl \
      libepoxy0 \
      libfile-basedir-perl \
      libfile-desktopentry-perl \
      libfile-listing-perl \
      libfile-mimeinfo-perl \
      libfont-afm-perl \
      libfontenc1 \
      libfribidi0 \
      libgbm1 \
      libgdk-pixbuf2.0-0 \
      libgdk-pixbuf2.0-bin \
      libgdk-pixbuf2.0-common \
      libgl1 \
      libgl1-mesa-dri \
      libglapi-mesa \
      libglvnd0 \
      libglx-mesa0 \
      libglx0 \
      libgtk-3-0 \
      libgtk-3-bin \
      libgtk-3-common \
      libhtml-form-perl \
      libhtml-format-perl \
      libhtml-parser-perl \
      libhtml-tagset-perl \
      libhtml-tree-perl \
      libhttp-cookies-perl \
      libhttp-daemon-perl \
      libhttp-date-perl \
      libhttp-message-perl \
      libhttp-negotiate-perl \
      libice6 \
      libicu66 \
      libio-html-perl \
      libio-socket-ssl-perl \
      libio-stringy-perl \
      libipc-system-simple-perl \
      libjbig0 \
      libjson-glib-1.0-0 \
      libjson-glib-1.0-common \
      libllvm12 \
      liblwp-mediatypes-perl \
      liblwp-protocol-https-perl \
      libmailtools-perl \
      libnet-dbus-perl \
      libnet-http-perl \
      libnet-smtp-ssl-perl \
      libnet-ssleay-perl \
      libpango-1.0-0 \
      libpangocairo-1.0-0 \
      libpangoft2-1.0-0 \
      libpciaccess0 \
      libpixman-1-0 \
      libproxy1v5 \
      librest-0.7-0 \
      librsvg2-2 \
      librsvg2-common \
      libsensors-config \
      libsensors5 \
      libsm6 \
      libsoup-gnome2.4-1 \
      libsoup2.4-1 \
      libtext-iconv-perl \
      libthai-data \
      libthai0 \
      libtie-ixhash-perl \
      libtiff5 \
      libtimedate-perl \
      libtry-tiny-perl \
      libu2f-udev \
      libudev1 \
      liburi-perl \
      libvulkan1 \
      libwayland-client0 \
      libwayland-cursor0 \
      libwayland-egl1 \
      libwayland-server0 \
      libwebp6 \
      libwww-perl \
      libwww-robotrules-perl \
      libx11-6 \
      libx11-data \
      libx11-protocol-perl \
      libx11-xcb1 \
      libxau6 \
      libxaw7 \
      libxcb-dri2-0 \
      libxcb-dri3-0 \
      libxcb-glx0 \
      libxcb-present0 \
      libxcb-randr0 \
      libxcb-render0 \
      libxcb-shape0 \
      libxcb-shm0 \
      libxcb-sync1 \
      libxcb-xfixes0 \
      libxcb1 \
      libxcomposite1 \
      libxcursor1 \
      libxdamage1 \
      libxdmcp6 \
      libxext6 \
      libxfixes3 \
      libxft2 \
      libxi6 \
      libxinerama1 \
      libxkbcommon0 \
      libxkbfile1 \
      libxml-parser-perl \
      libxml-twig-perl \
      libxml-xpathengine-perl \
      libxml2 \
      libxmu6 \
      libxmuu1 \
      libxpm4 \
      libxrandr2 \
      libxrender1 \
      libxshmfence1 \
      libxt6 \
      libxtst6 \
      libxv1 \
      libxxf86dga1 \
      libxxf86vm1 \
      mesa-vulkan-drivers \
      perl-openssl-defaults \
      shared-mime-info \
      tzdata \
      ubuntu-mono \
      udev \
      x11-common \
      x11-utils \
      x11-xserver-utils \
      xdg-utils \
      xkb-data \
      && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
