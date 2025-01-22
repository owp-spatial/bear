from bear.typing import Provider
from typing import KeysView, ItemsView


class ProviderRegistryMeta(type):
    _registry: dict[str, Provider] = {}

    def __getattr__(self, name):
        try:
            return self._registry[name]
        except KeyError:
            raise AttributeError(name)


class ProviderRegistry(metaclass=ProviderRegistryMeta):
    """Registry of Available Providers

    This class provides static methods for registering and accessing
    registered provider classes. The goal is to encapsulate provider
    modules so that access to those modules is done via this registry.

    A module may register itself by calling::

        ProviderRegistry.register('mymodule', my_provider_obj)

    Then, it can be accessed by calling::

        ProviderRegistry.get('mymodule')
        #> <my_provider_obj>

    The ProviderRegistry class cannot be instantiated.
    """

    def __init__(self):
        raise RuntimeError("ProviderRegistry cannot be instantiated.")

    @staticmethod
    def register(
        name: str, provider: Provider, *, overwrite: bool = False
    ) -> None:
        """Register a provider into the registry.

        Parameters
        ----------
        name : str
            Key/name of the provider.
        provider : Provider
            Provider object that implements the Provider protocol.
        overwrite : bool, optional
            If `name` already exists and overwrite is True, then overwrite the
            provider `name` with object of `provider`, by default False.

        Raises
        ------
        KeyError
            If `overwrite` if False and `name` already exists in the registry.
        """

        if name in ProviderRegistry._registry and not overwrite:
            raise KeyError(f"Provider with name {name} already registered.")

        ProviderRegistry._registry[name] = provider

    @staticmethod
    def providers() -> KeysView[str]:
        """Retrieve a view of all registered providers.

        Returns
        -------
        KeysView[str]
            Iterable sequence of provider keys.
        """

        return ProviderRegistry._registry.keys()

    @staticmethod
    def iter() -> ItemsView[str, Provider]:
        """Iterate over all registered providers.

        Returns
        -------
        ItemsView[str, Provider]
            Iterable view of tuples of provider keys and types.
        """

        return ProviderRegistry._registry.items()

    @staticmethod
    def get(key: str) -> Provider:
        """Retrieve a provider type by key.

        Parameters
        ----------
        key : str
            Provider key/name.

        Returns
        -------
        Provider
            Associated provider.
        """

        return ProviderRegistry._registry[key]


__all__ = ("ProviderRegistry",)
