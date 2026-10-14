{
  description = "Development environment for the Kafka learning project and PaySim pipeline";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { nixpkgs, ... }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      eachSystem = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = eachSystem (system:
        let
          pkgs = import nixpkgs { inherit system; };
          pythonBase = pkgs.python312.override {
            packageOverrides = final: prev: {
              # AnyIO's upstream check suite currently fails in this nixpkgs revision;
              # it is only a transitive build dependency here.
              anyio = prev.anyio.overridePythonAttrs (_: { doCheck = false; });
            };
          };
          python = pythonBase.withPackages (packages: with packages; [
            pandas
            pyarrow
            onnx
          ]);
        in
        {
          default = pkgs.mkShell {
            packages = [ python pkgs.go pkgs.onnxruntime pkgs.pkg-config ];
            shellHook = ''
              export ONNXRUNTIME_SHARED_LIBRARY_PATH="${pkgs.onnxruntime}/lib/${if pkgs.stdenv.isDarwin then "libonnxruntime.dylib" else "libonnxruntime.so"}"
              export CGO_ENABLED=1
            '';
          };

          paysim = pkgs.mkShell {
            packages = [ python pkgs.go pkgs.onnxruntime pkgs.pkg-config ];
            shellHook = ''
              export ONNXRUNTIME_SHARED_LIBRARY_PATH="${pkgs.onnxruntime}/lib/${if pkgs.stdenv.isDarwin then "libonnxruntime.dylib" else "libonnxruntime.so"}"
              export CGO_ENABLED=1
            '';
          };
        });
    };
}
