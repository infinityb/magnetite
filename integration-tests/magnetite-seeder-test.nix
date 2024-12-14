{ magnetite, nixpkgsSrc, ... }:
import "${nixpkgsSrc}/nixos/tests/make-test-python.nix" (
  { pkgs, ... }:
    let
      # Some random file to serve.
      served_file = pkgs.runCommand "example.tar" {} ''
        ${pkgs.gnutar}/bin/tar cvf "$out" ${pkgs.ffmpeg.src}
      '';

      # the torrent file for said file.
      served_file_torrent = pkgs.runCommand "example.tar.torrent" {} ''
        cp ${served_file} ./example.tar
        ${pkgs.transmission}/bin/transmission-create \
          --private ./example.tar \
          --tracker http://${internalClientAddressTracker}:6969/announce \
          --outfile "$out"
      '';

      internalRouterAddress = "192.168.80.1";
      internalClientAddressTracker = "192.168.80.2";
      internalClientAddressSeeder = "192.168.80.3";
      internalClientAddressDownloader1 = "192.168.80.4";
      internalClientAddressDownloader2 = "192.168.80.5";

      download-dir = "/var/lib/transmission/Downloads";
      transmissionConfig = { ... }: {
        environment.systemPackages = [ pkgs.transmission ];
        services.transmission = {
          enable = true;
          settings = {
            dht-enabled = false;
            message-level = 2;
            inherit download-dir;
          };
        };
      };
    in
    {
      name = "magnetite-tracker-integration-with-transmission";
      meta = with pkgs.lib.maintainers; {
        maintainers = [];
      };

      nodes = {
        tracker = { pkgs, ... }: {
          imports = [];

          virtualisation.vlans = [ 2 ];
          networking.interfaces.eth0.ipv4.addresses = [];
          networking.interfaces.eth1.ipv4.addresses = [
            { address = internalClientAddressTracker; prefixLength = 24; }
          ];
          # networking.defaultGateway = internalRouterAddress;
          networking.firewall.enable = false;

          systemd.services.magnetite-tracker = {
            description = "Magnetite Tracker";
            wantedBy = [ "multi-user.target" ];
            after = [ "network.target" ];
            requires = [ "network.target" ];
            script = ''
              export RUST_BACKTRACE=1
              ${magnetite.workspaceMembers.magnetite-tracker.build}/bin/magnetite-tracker \
                -vvvvv --allow-all \
                --bind-address 0.0.0.0:6969 
            '';
          };
        };

        seeder = { pkgs, ... }: {
          imports = [ transmissionConfig ];
          environment.systemPackages = [];

          virtualisation.vlans = [ 2 ];
          networking.interfaces.eth0.ipv4.addresses = [];
          networking.interfaces.eth1.ipv4.addresses = [
            { address = internalClientAddressSeeder; prefixLength = 24; }
          ];

          systemd.services.magnetite-aggregator = {
            description = "Magnetite Aggregator";
            wantedBy = [ "multi-user.target" ];
            after = [ "network.target" ];
            requires = [ "network.target" ];
            script = ''
              export RUST_BACKTRACE=1
              ${magnetite.workspaceMembers.magnetite-single-aggregator.build}/bin/magnetite-single-aggregator -vvvvv
            '';
          };

          systemd.services.magnetite-datapath = {
            description = "Magnetite Datapath";
            wantedBy = [ "multi-user.target" ];
            after = [ "network.target" ];
            requires = [ "network.target" ];
            script = ''
              export RUST_BACKTRACE=1
              export PATH="$PATH:${magnetite.workspaceMembers.magnetite-single.build}/bin"
              ${magnetite.workspaceMembers.magnetite-single-wrapper.build}/bin/magnetite-single-wrapper
            '';
          };

          # networking.defaultGateway = internalRouterAddress;
          networking.firewall.enable = false;
        };

        downloader1 = { pkgs, ... }: {
          imports = [ transmissionConfig ];

          virtualisation.vlans = [ 2 ];
          networking.interfaces.eth0.ipv4.addresses = [];
          networking.interfaces.eth1.ipv4.addresses = [
            { address = internalClientAddressDownloader1; prefixLength = 24; }
          ];

          # networking.defaultGateway = internalRouterAddress;
          networking.firewall.enable = false;
        };

        downloader2 = { pkgs, ... }: {
          imports = [ transmissionConfig ];

          virtualisation.vlans = [ 2 ];
          networking.interfaces.eth0.ipv4.addresses = [];
          networking.interfaces.eth1.ipv4.addresses = [
            { address = internalClientAddressDownloader2; prefixLength = 24; }
          ];
          # networking.defaultGateway = internalRouterAddress;
          networking.firewall.enable = false;
        };
      };

      testScript = { nodes, ... }: ''
          start_all()

          # wait for network on all 3 nodes to come up
          tracker.wait_for_unit("network-online.target")
          seeder.wait_for_unit("network-online.target")
          downloader1.wait_for_unit("network-online.target")

          # critical services
          tracker.wait_for_unit("magnetite-tracker")
          tracker.wait_for_open_port(6969)
          seeder.wait_for_unit("magnetite-aggregator")
          seeder.wait_for_unit("magnetite-datapath")
          seeder.wait_for_open_port(51409)
          for s in [downloader1, downloader2]:
              s.wait_for_unit("transmission")

          # the meat.
          seeder.succeed("mkdir ${download-dir}/data")
          seeder.succeed("cp ${served_file} ${download-dir}/data/example.tar")
          seeder.succeed("${magnetite.workspaceMembers.magnetite-single-aggregator.build}/bin/magnetite-single-client ${served_file_torrent}")
          downloader1.succeed("transmission-remote --add ${served_file_torrent} --no-portmap --no-dht --download-dir ${download-dir}/data")
          downloader1.wait_for_file("${download-dir}/data/example.tar")
          downloader1.succeed("cmp ${download-dir}/data/example.tar ${served_file}")
          seeder.stop_job("magnetite-aggregator")
          seeder.stop_job("magnetite-datapath")

          downloader2.succeed("transmission-remote --add ${served_file_torrent} --no-portmap --no-dht --download-dir ${download-dir}/data")
          downloader2.wait_for_file("${download-dir}/data/example.tar")
          downloader2.succeed("cmp ${download-dir}/data/example.tar ${served_file}")

          downloader2.succeed("false")
        '';
    }
  )
