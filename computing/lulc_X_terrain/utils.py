import ee
from .terrain_classifier import GEETerrainClassifier

aez_lulcXterrain_cluster_centroids = {
    "aez1": {
        "plains": {
            "0": {
                "cluster_name": "~40% Barrenlands",
                "cluster_vector": [
                    3.91740236e-01,
                    2.52998730e-03,
                    1.11149390e-03,
                    6.72869010e-03,
                    1.21055601e-04,
                    5.82671948e-03,
                    0.00000000e00,
                ],
            },
            "1": {
                "cluster_name": "~72% Barrenlands",
                "cluster_vector": [
                    7.63053021e-01,
                    -3.25260652e-19,
                    1.05852006e-06,
                    1.42630744e-06,
                    6.15078999e-05,
                    -3.03576608e-18,
                    0.00000000e00,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~26% Barrenlands",
                "cluster_vector": [2.69299718e-01, 0.00000000e00, 6.56242950e-06],
            },
            "1": {
                "cluster_name": "~63% Barrenlands",
                "cluster_vector": [6.33014898e-01, 0.00000000e00, 0.00000000e00],
            },
        },
    },
    "aez2": {
        "plains": {
            "0": {
                "cluster_name": "Mostly Double Cropped(~79%)",
                "cluster_vector": [
                    2.01348289e-02,
                    7.42766457e-01,
                    1.24918120e-02,
                    2.59465109e-02,
                    3.01885568e-03,
                    5.16214522e-02,
                    3.36112314e-03,
                ],
            },
            "1": {
                "cluster_name": "Mostly Barrenlands(~67%)",
                "cluster_vector": [
                    5.92784228e-01,
                    7.21967152e-02,
                    4.57961374e-02,
                    6.74789191e-02,
                    2.06400342e-03,
                    1.64097492e-02,
                    2.39182270e-05,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "Mostly Barrenlands (~25%)",
                "cluster_vector": [2.59968768e-01, 4.64676009e-05, 7.03727384e-05],
            },
            "1": {
                "cluster_name": "Mostly Tree/Forests (~22%)",
                "cluster_vector": [7.72795703e-03, 1.32532047e-02, 2.93186560e-01],
            },
        },
    },
    "aez3": {
        "plains": {
            "0": {
                "cluster_name": "Mostly Single Cropped(~51%)",
                "cluster_vector": [
                    0.01306332,
                    0.13665998,
                    0.10002133,
                    0.52574991,
                    0.00947106,
                    0.13754808,
                    0.00627313,
                ],
            },
            "1": {
                "cluster_name": "Cropped(~30%) and Tree/Forests(~18%), Shrubs&Scrubs(~20%)",
                "cluster_vector": [
                    0.00937157,
                    0.13991859,
                    0.21233056,
                    0.16555839,
                    0.00165823,
                    0.19635352,
                    0.0126126,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "Mostly Tree/Forests (~35%)",
                "cluster_vector": [0.01741352, 0.07512888, 0.38487854],
            },
            "1": {
                "cluster_name": "Tree/Forests(~14%) and Shrubs&Scrubs(~15%)",
                "cluster_vector": [0.00285718, 0.12866369, 0.12766821],
            },
        },
    },
    "aez4": {
        "plains": {
            "0": {
                "cluster_name": "~67% Double Cropped",
                "cluster_vector": [
                    4.12689961e-03,
                    6.57794553e-01,
                    7.79271283e-04,
                    1.52015770e-02,
                    2.45006065e-03,
                    6.68075188e-02,
                    1.34670108e-02,
                ],
            },
            "1": {
                "cluster_name": "25% Cropped and 20% Tree/Forests",
                "cluster_vector": [
                    4.66533381e-03,
                    9.89368180e-02,
                    7.39647030e-02,
                    5.37652597e-02,
                    2.57175783e-04,
                    1.82912004e-01,
                    9.71718807e-03,
                ],
            },
            "2": {
                "cluster_name": "Mostly Trees/Forests(~53%)",
                "cluster_vector": [
                    1.28339663e-03,
                    8.17495668e-02,
                    2.11021936e-02,
                    3.24100803e-02,
                    3.41328294e-05,
                    5.58292563e-01,
                    8.84843208e-02,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~57% Tree/Forests",
                "cluster_vector": [0.00469479, 0.07735867, 0.57990417],
            },
            "1": {
                "cluster_name": "~32% Tree/Forests",
                "cluster_vector": [0.00423429, 0.09585884, 0.28480174],
            },
        },
    },
    "aez5": {
        "plains": {
            "0": {
                "cluster_name": "Mostly Cropped(~71%)",
                "cluster_vector": [
                    0.00561504,
                    0.30217839,
                    0.02412776,
                    0.44026556,
                    0.01866382,
                    0.10488088,
                    0.01543984,
                ],
            },
            "1": {
                "cluster_name": "Mostly Single Cropped(~70%)",
                "cluster_vector": [
                    0.00606159,
                    0.1343815,
                    0.01819084,
                    0.72275595,
                    0.01032322,
                    0.05453541,
                    0.0017029,
                ],
            },
            "2": {
                "cluster_name": "Mostly Double Cropped (~60%)",
                "cluster_vector": [
                    0.0009554,
                    0.5734176,
                    0.00276574,
                    0.18098238,
                    0.00857198,
                    0.09868609,
                    0.03767174,
                ],
            },
            "3": {
                "cluster_name": "Tree/Forests(~39%) and Cropped(~30%)",
                "cluster_vector": [
                    0.02027753,
                    0.16281436,
                    0.03852361,
                    0.1157409,
                    0.02666546,
                    0.35722303,
                    0.03605709,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~18% Tree/Forests",
                "cluster_vector": [0.00301327, 0.01132139, 0.19362293],
            },
            "1": {
                "cluster_name": "~58% Tree/Forests",
                "cluster_vector": [0.00200727, 0.01770298, 0.57877844],
            },
        },
    },
    "aez6": {
        "plains": {
            "0": {
                "cluster_name": "Mix of Double Cropped(~18%), Single Cropped(~30%) and Tree/Forests(~33%)",
                "cluster_vector": [
                    0.01403629,
                    0.20766691,
                    0.04120911,
                    0.27180845,
                    0.00430812,
                    0.26633672,
                    0.01640329,
                ],
            },
            "1": {
                "cluster_name": "Mostly Trees/Forests(~63%)",
                "cluster_vector": [
                    0.0014036,
                    0.05772735,
                    0.03651197,
                    0.0901065,
                    0.00112734,
                    0.65107751,
                    0.0036425,
                ],
            },
            "2": {
                "cluster_name": "Mostly Single Cropped(~58%)",
                "cluster_vector": [
                    0.01091959,
                    0.11827529,
                    0.03902514,
                    0.57992466,
                    0.00742423,
                    0.13262949,
                    0.00329989,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~24% Tree/Forests",
                "cluster_vector": [7.62981429e-04, 1.00140366e-02, 2.22964093e-01],
            },
            "1": {
                "cluster_name": "~43% Tree/Forests",
                "cluster_vector": [1.08420217e-19, 9.65352345e-03, 4.35096623e-01],
            },
        },
    },
    "aez7": {
        "plains": {
            "0": {
                "cluster_name": "Mostly Tree/Forests(~62%)",
                "cluster_vector": [
                    5.46786997e-04,
                    6.58227180e-02,
                    3.84749664e-02,
                    2.66151129e-02,
                    1.97989119e-03,
                    6.68779022e-01,
                    3.30755765e-02,
                ],
            },
            "1": {
                "cluster_name": "~51% Cropped and ~16% Trees/Forests",
                "cluster_vector": [
                    7.24512783e-02,
                    2.02932148e-01,
                    4.90984952e-02,
                    3.17094650e-01,
                    5.55725974e-03,
                    1.63123053e-01,
                    8.16588777e-03,
                ],
            },
            "2": {
                "cluster_name": "~35% Cropped and ~40% Tree/Forests",
                "cluster_vector": [
                    1.66012886e-02,
                    1.68892738e-01,
                    4.44911223e-02,
                    1.06577333e-01,
                    1.84594919e-03,
                    3.84131310e-01,
                    2.07526495e-02,
                ],
            },
            "3": {
                "cluster_name": "Mostly Double Cropped(~40%)",
                "cluster_vector": [
                    3.03883170e-02,
                    4.51208874e-01,
                    1.84744187e-02,
                    1.46455612e-01,
                    4.83300359e-03,
                    1.77037130e-01,
                    2.80882782e-02,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~24% Tree/Forests",
                "cluster_vector": [0.01562469, 0.00453486, 0.22450814],
            },
            "1": {
                "cluster_name": "~68% Tree/Forests",
                "cluster_vector": [0.0, 0.00810459, 0.69723081],
            },
        },
    },
    "aez8": {
        "plains": {
            "0": {
                "cluster_name": "Mix of Single Cropped(~33%) and Tree/Forests(~23%)",
                "cluster_vector": [
                    0.02139623,
                    0.07537394,
                    0.09474202,
                    0.35723851,
                    0.09394298,
                    0.21530148,
                    0.04109681,
                ],
            },
            "1": {
                "cluster_name": "Mostly Trees/Forests(~48%)",
                "cluster_vector": [
                    0.00688771,
                    0.14155059,
                    0.0253851,
                    0.06726722,
                    0.00904172,
                    0.47462431,
                    0.08667089,
                ],
            },
            "2": {
                "cluster_name": "Mix of Double Cropped(~39%) and Tree/Forests(~28%)",
                "cluster_vector": [
                    0.00788156,
                    0.39868009,
                    0.024398,
                    0.0757545,
                    0.00440524,
                    0.24314599,
                    0.11480779,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~15% Tree/Forests",
                "cluster_vector": [1.70501711e-03, 7.78480497e-03, 1.82373997e-01],
            },
            "1": {
                "cluster_name": "~53% Tree/Forests",
                "cluster_vector": [4.49913322e-05, 4.40921757e-05, 5.61415262e-01],
            },
        },
    },
    "aez9": {
        "plains": {
            "0": {
                "cluster_name": "~50% Double Cropped and ~27% Tree/Forests",
                "cluster_vector": [
                    6.96747992e-03,
                    5.34708868e-01,
                    6.14998102e-04,
                    1.13543154e-02,
                    2.87997687e-03,
                    2.70783043e-01,
                    6.22243966e-02,
                ],
            },
            "1": {
                "cluster_name": "Mostly Trees/Forests(~73%)",
                "cluster_vector": [
                    8.62676071e-03,
                    6.69496269e-02,
                    7.86528942e-04,
                    4.11892506e-02,
                    2.38452086e-03,
                    7.20193540e-01,
                    6.70412747e-02,
                ],
            },
            "2": {
                "cluster_name": "~22% Double Cropped and ~46% Tree/Forests",
                "cluster_vector": [
                    1.55388846e-02,
                    2.23429750e-01,
                    3.19838448e-03,
                    3.26170536e-02,
                    1.56903785e-03,
                    4.36671683e-01,
                    5.67921520e-02,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~20% Tree/Forests",
                "cluster_vector": [1.99205908e-03, 5.01022018e-04, 2.20813189e-01],
            },
            "1": {
                "cluster_name": "~55% Tree/Forests",
                "cluster_vector": [3.23093396e-03, 6.26841892e-04, 6.00155221e-01],
            },
        },
    },
    "aez10": {
        "plains": {
            "0": {
                "cluster_name": "~43% Double Cropped and ~23% Single Cropped",
                "cluster_vector": [
                    1.32050502e-02,
                    3.99646283e-01,
                    5.87549476e-02,
                    2.22176697e-01,
                    1.10953406e-03,
                    1.12360233e-01,
                    2.73198227e-03,
                ],
            },
            "1": {
                "cluster_name": "Mostly Trees/Forests(~41%)",
                "cluster_vector": [
                    3.02983980e-04,
                    5.84399358e-02,
                    6.20491014e-02,
                    8.53326162e-02,
                    5.75671928e-05,
                    4.50317053e-01,
                    3.04143337e-04,
                ],
            },
            "2": {
                "cluster_name": "Mostly Double Cropped(~73%)",
                "cluster_vector": [
                    3.30448952e-03,
                    7.20085103e-01,
                    9.06477138e-03,
                    6.19126743e-02,
                    8.12760408e-04,
                    1.16157102e-01,
                    4.83423148e-03,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~43% Tree/Forests",
                "cluster_vector": [4.81502477e-04, 3.61917076e-02, 4.39994818e-01],
            },
            "1": {
                "cluster_name": "~12% Tree/Forests",
                "cluster_vector": [9.94276651e-05, 4.60047537e-02, 1.80099284e-01],
            },
        },
    },
    "aez11": {
        "plains": {
            "0": {
                "cluster_name": "Mostly Single Cropped(~43%)",
                "cluster_vector": [
                    0.00655377,
                    0.18143284,
                    0.0511778,
                    0.42633951,
                    0.00423132,
                    0.14364151,
                    0.00200144,
                ],
            },
            "1": {
                "cluster_name": "Mostly Tree/Forests(~45%)",
                "cluster_vector": [
                    0.0009164,
                    0.06198753,
                    0.02407041,
                    0.02960597,
                    0.00152359,
                    0.45308141,
                    0.00569879,
                ],
            },
            "2": {
                "cluster_name": "Mix of Double Cropped(~20%), Single Cropped(~20%) and Tree/Forests(~30%)",
                "cluster_vector": [
                    0.00801883,
                    0.22130974,
                    0.07919231,
                    0.18699913,
                    0.00308643,
                    0.25241146,
                    0.00295972,
                ],
            },
            "3": {
                "cluster_name": "Mix of Double Cropped(~5%) and Tree/Forests(~15%)",
                "cluster_vector": [
                    0.00341205,
                    0.06325032,
                    0.04443115,
                    0.06829491,
                    0.00088178,
                    0.15912605,
                    0.00703504,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~35% Tree/Forests",
                "cluster_vector": [1.09358425e-03, 2.17215320e-02, 2.89168707e-01],
            },
            "1": {
                "cluster_name": "~60% Tree/Forests",
                "cluster_vector": [7.85093258e-05, 6.76859492e-03, 5.96140163e-01],
            },
        },
    },
    "aez12": {
        "plains": {
            "0": {
                "cluster_name": "Mostly Single Cropped(~43%)",
                "cluster_vector": [
                    0.0093753,
                    0.1253042,
                    0.0990157,
                    0.43538976,
                    0.00235512,
                    0.17217369,
                    0.00110297,
                ],
            },
            "1": {
                "cluster_name": "Mostly Trees/Forests(~36%)",
                "cluster_vector": [
                    0.00403924,
                    0.08690656,
                    0.05636342,
                    0.09459072,
                    0.00111222,
                    0.35382808,
                    0.00403482,
                ],
            },
            "2": {
                "cluster_name": "Mix of Cropped(~55%) and Trees/Forests(~25%)",
                "cluster_vector": [
                    0.00465512,
                    0.27904105,
                    0.03561681,
                    0.25762406,
                    0.00404674,
                    0.23938812,
                    0.00554456,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~25% Tree/Forests",
                "cluster_vector": [2.48613368e-04, 1.16666762e-02, 2.80314583e-01],
            },
            "1": {
                "cluster_name": "~60% Tree/Forests",
                "cluster_vector": [6.47252603e-04, 3.84558341e-03, 6.44240276e-01],
            },
        },
    },
    "aez13": {
        "plains": {
            "0": {
                "cluster_name": "~43% Double Cropped and ~24% Tree/Forests",
                "cluster_vector": [
                    1.31670304e-02,
                    4.42433652e-01,
                    6.09822815e-04,
                    8.76388345e-02,
                    7.49370639e-03,
                    2.93492386e-01,
                    1.94643543e-02,
                ],
            },
            "1": {
                "cluster_name": "Mostly Double Cropped(~71%)",
                "cluster_vector": [
                    8.20090087e-03,
                    7.00167838e-01,
                    4.45480172e-04,
                    4.03444688e-02,
                    3.42042059e-03,
                    1.31195793e-01,
                    4.76082352e-03,
                ],
            },
            "2": {
                "cluster_name": "Mostly Tree/Forests(~53%)",
                "cluster_vector": [
                    6.57004678e-03,
                    1.49257238e-01,
                    2.25996761e-03,
                    1.39403415e-01,
                    1.64688367e-03,
                    5.22935905e-01,
                    2.04999627e-02,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~25% Tree/Forests",
                "cluster_vector": [0.00642259, 0.00094345, 0.2603422],
            },
            "1": {
                "cluster_name": "~53% Tree/Forests",
                "cluster_vector": [0.02422837, 0.00230161, 0.54344944],
            },
        },
    },
    "aez14": {
        "plains": {
            "0": {
                "cluster_name": "~15% Tree/Forests",
                "cluster_vector": [
                    6.34419254e-02,
                    5.99441170e-02,
                    6.46435965e-03,
                    7.12996909e-02,
                    2.37282997e-03,
                    1.19506228e-01,
                    1.40493168e-02,
                ],
            },
            "1": {
                "cluster_name": "~45% Tree/Forests and ~18% Double Cropping",
                "cluster_vector": [
                    1.12055466e-02,
                    1.64312464e-01,
                    1.96006961e-03,
                    4.87704435e-02,
                    3.03239571e-04,
                    4.80021636e-01,
                    2.41203181e-02,
                ],
            },
            "2": {
                "cluster_name": "~55% Double Cropping and ~20% Tree/Forests",
                "cluster_vector": [
                    1.91310921e-02,
                    5.70426678e-01,
                    1.31765281e-03,
                    1.81890361e-02,
                    1.73438184e-03,
                    2.34428137e-01,
                    3.29955648e-02,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~20% Tree/Forests",
                "cluster_vector": [4.55659571e-02, 7.47930580e-03, 2.05242426e-01],
            },
            "1": {
                "cluster_name": "~75% Tree/Forests",
                "cluster_vector": [5.51292992e-04, 2.89698053e-03, 7.14949093e-01],
            },
        },
    },
    "aez15": {
        "plains": {
            "0": {
                "cluster_name": "Mostly Tree/Forests(~71%)",
                "cluster_vector": [
                    0.00765383639,
                    0.0623749603,
                    0.00124701912,
                    0.0381570142,
                    0.000672155796,
                    0.728323716,
                    0.0142319500,
                ],
            },
            "1": {
                "cluster_name": "20% Double Cropped and 45% Tree/Forests",
                "cluster_vector": [
                    0.00975431939,
                    0.207892035,
                    0.00198746319,
                    0.0917496565,
                    0.00227160154,
                    0.411400172,
                    0.0206571148,
                ],
            },
            "2": {
                "cluster_name": "50% Single Cropped and 30% Tree/Forests",
                "cluster_vector": [
                    0.00238781273,
                    0.115375359,
                    0.000597808747,
                    0.466760706,
                    0.000761817635,
                    0.274699288,
                    0.00522228928,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~31% Tree/Forests",
                "cluster_vector": [0.00100817944, 0.000483109427, 0.276487255],
            },
            "1": {
                "cluster_name": "~70% Tree/Forests",
                "cluster_vector": [0.000205475837, 0.0000550464006, 0.72818187],
            },
        },
    },
    "aez16": {
        "plains": {
            "0": {
                "cluster_name": "~75% Tree/Forests",
                "cluster_vector": [
                    2.56001838e-02,
                    4.95093978e-02,
                    3.56897997e-03,
                    1.86945133e-02,
                    6.08167323e-04,
                    7.06253837e-01,
                    3.85975854e-03,
                ],
            },
            "1": {
                "cluster_name": "~30% Tree/Forests",
                "cluster_vector": [
                    4.53680332e-02,
                    9.09786176e-02,
                    3.91818377e-03,
                    6.23760754e-02,
                    3.66915627e-03,
                    2.89107179e-01,
                    3.51285707e-03,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~23% Tree/Forests",
                "cluster_vector": [2.76621233e-02, 1.86619793e-03, 2.54287316e-01],
            },
            "1": {
                "cluster_name": "~82% Tree/Forests",
                "cluster_vector": [1.21103132e-03, 9.31499539e-05, 7.84482300e-01],
            },
        },
    },
    "aez17": {
        "plains": {
            "0": {
                "cluster_name": "~25% Tree/Forests",
                "cluster_vector": [
                    3.70591326e-03,
                    1.08462417e-02,
                    5.12833748e-04,
                    7.66811312e-03,
                    6.25061996e-05,
                    2.27589941e-01,
                    1.48908883e-03,
                ],
            },
            "1": {
                "cluster_name": "~58% Tree/Forests",
                "cluster_vector": [
                    9.26691593e-04,
                    2.01079101e-02,
                    6.07356676e-04,
                    2.37301326e-02,
                    2.04758803e-04,
                    5.97149098e-01,
                    5.11182599e-03,
                ],
            },
            "2": {
                "cluster_name": "Mostly Single Cropped(50%)",
                "cluster_vector": [
                    2.95370806e-04,
                    6.55940937e-02,
                    2.97411424e-04,
                    5.22921502e-01,
                    9.72395517e-05,
                    2.23620507e-01,
                    2.44065902e-03,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~76% Tree/Forests",
                "cluster_vector": [7.22939801e-05, 2.15881386e-04, 7.60146818e-01],
            },
            "1": {
                "cluster_name": "~40% Tree/Forests",
                "cluster_vector": [1.34914784e-03, 4.05703738e-04, 3.76754019e-01],
            },
        },
    },
    "aez18": {
        "plains": {
            "0": {
                "cluster_name": "~22% Double Cropped and ~25% Tree/Forests",
                "cluster_vector": [
                    0.00920583,
                    0.21101496,
                    0.00622576,
                    0.1222924,
                    0.01174155,
                    0.25025426,
                    0.01427558,
                ],
            },
            "1": {
                "cluster_name": "~45% Tree/Forests",
                "cluster_vector": [
                    0.0009608,
                    0.0820932,
                    0.00491651,
                    0.02124451,
                    0.00151243,
                    0.48051711,
                    0.0319512,
                ],
            },
            "2": {
                "cluster_name": "~45% Double Cropped and ~30% Tree/Forests",
                "cluster_vector": [
                    0.00214112,
                    0.51037186,
                    0.00054234,
                    0.04519692,
                    0.00946847,
                    0.31394289,
                    0.00827232,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~50% Tree/Forests",
                "cluster_vector": [6.95437331e-04, 1.14879918e-03, 5.29195174e-01],
            },
            "1": {
                "cluster_name": "~22% Tree/Forests",
                "cluster_vector": [3.44240252e-04, 1.62071994e-03, 2.39787378e-01],
            },
        },
    },
    "aez19": {
        "plains": {
            "0": {
                "cluster_name": "Mostly Tree/Forests(~45%)",
                "cluster_vector": [
                    3.17673004e-04,
                    2.00204724e-02,
                    9.64252814e-03,
                    9.57021472e-03,
                    5.75632179e-04,
                    4.32821364e-01,
                    8.82607300e-03,
                ],
            },
            "1": {
                "cluster_name": "Mix of Double Cropped(~20%), Single Cropped(~10%) and Tree/Forests(~34%)",
                "cluster_vector": [
                    7.16909966e-03,
                    2.08115262e-01,
                    3.75920654e-02,
                    1.14364295e-01,
                    3.38211613e-03,
                    3.38537313e-01,
                    1.55060682e-02,
                ],
            },
            "2": {
                "cluster_name": "~20% Tree Forests",
                "cluster_vector": [
                    5.07113347e-05,
                    2.13995057e-03,
                    2.97582642e-02,
                    6.51768193e-03,
                    1.19491640e-04,
                    1.73512297e-01,
                    4.56980000e-03,
                ],
            },
            "3": {
                "cluster_name": "Mostly Tree/Forests(~65%)",
                "cluster_vector": [
                    1.75237200e-04,
                    5.31286985e-02,
                    8.49418862e-03,
                    1.47039178e-02,
                    6.25218013e-04,
                    6.63786281e-01,
                    1.36879305e-02,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~40% Tree/Forests",
                "cluster_vector": [2.25023278e-04, 1.32452895e-03, 3.88924736e-01],
            },
            "1": {
                "cluster_name": "~70% Tree/Forests",
                "cluster_vector": [5.49747987e-05, 1.70343937e-04, 7.11185509e-01],
            },
        },
    },
    "aez20": {
        "plains": {
            "0": {
                "cluster_name": "~20% Tree/Forests",
                "cluster_vector": [
                    4.21141086e-04,
                    1.68352043e-03,
                    1.92961955e-05,
                    4.68009156e-04,
                    3.37351908e-04,
                    2.30715127e-01,
                    4.74701552e-03,
                ],
            },
            "1": {
                "cluster_name": "~60% Tree/Forests",
                "cluster_vector": [
                    2.52062122e-04,
                    2.37674839e-03,
                    3.15910027e-04,
                    4.75441670e-04,
                    2.34724811e-04,
                    6.30138997e-01,
                    4.50682574e-03,
                ],
            },
        },
        "slopes": {
            "0": {
                "cluster_name": "~70% Tree/Forests",
                "cluster_vector": [2.18373621e-04, 2.11028983e-05, 7.07072450e-01],
            },
            "1": {
                "cluster_name": "~40% Tree/Forests",
                "cluster_vector": [1.33907247e-04, 4.89202029e-05, 4.08239274e-01],
            },
        },
    },
}


def process_mws(mws_fc):
    """
    Process watershed FeatureCollection and assign terrain clusters

    Parameters:
    watersheds_fc: ee.FeatureCollection - Input watersheds

    Returns:
    ee.FeatureCollection - Watersheds with terrain clusters assigned
    """
    # Filter by area (400 hectares = 4000000 m²)
    mwsheds = mws_fc.map(lambda f: f.set("area", f.geometry().area())).filter(
        ee.Filter.gt("area", 4000000)
    )

    # Create classifier and assign clusters
    classifier = GEETerrainClassifier()
    return mwsheds.map(classifier.assign_cluster)


def calculate_area(mask, study_area):
    """
    Calculates area of masked regions with correct band handling.

    Args:
        mask: ee.Image of binary mask
        study_area: ee.Geometry of the region to analyze

    Returns:
        ee.Number: Area in square kilometers
    """
    # Ensure mask is binary and name the band
    area_image = mask.gt(0).multiply(ee.Image.pixelArea()).rename("area")

    # Calculate area using the correct band name
    area = area_image.reduceRegion(
        reducer=ee.Reducer.sum(), geometry=study_area, scale=30, maxPixels=1e10
    )

    return ee.Number(area.get("area")).divide(1e6)
