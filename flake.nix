{
  description = "Application packaged using poetry2nix";
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    poetry2nix = {
      url = "github:nix-community/poetry2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, poetry2nix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        # see https://github.com/nix-community/poetry2nix/tree/master#api for more functions and examples.
        pkgs = nixpkgs.legacyPackages.${system};
        # https://github.com/nix-community/poetry2nix/issues/1457#issuecomment-1848267087
        inherit (poetry2nix.lib.mkPoetry2Nix { inherit pkgs; }) mkPoetryApplication defaultPoetryOverrides;
      in
      {
        packages = {
          myapp = mkPoetryApplication { 
             projectDir = self;
             preferWheels = true;
             # https://github.com/nix-community/poetry2nix/blob/master/docs/edgecases.md#modulenotfounderror-no-module-named-packagename
             overrides = defaultPoetryOverrides.extend
              (self: super: {
                atpublic = super.atpublic.overridePythonAttrs
                (
                  old: {
                    buildInputs = (old.buildInputs or [ ]) ++ [super.hatchling];
                  }
                );
                ibis-framework = super.ibis-framework.overridePythonAttrs
                (
                  old: {
                    buildInputs = (old.buildInputs or [ ]) ++ [ super.poetry-dynamic-versioning ];
                  }
                );
              });
          };
          default = self.packages.${system}.myapp;
        };
        devShells.default = pkgs.mkShell {
          inputsFrom = [ self.packages.${system}.myapp ];
          packages = [ pkgs.poetry ];
        };
      });
}
